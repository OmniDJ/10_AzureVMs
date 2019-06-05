import subprocess
import os
import re
import json
import logging
import shutil
import redis
import websockets
import asyncio

from time import sleep, time

from django.conf import settings

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.subscription import SubscriptionClient

from vapor_logger import get_logger
from processapp.process_utils import ws_send_next_stage

from .azure_helper.azure_vm_helper import AzureSubscriptionDebugger
from .azure_helper.logger_helper import LoadLogger


class AzureVM:
    """ Interact with the Azure virtual machines """

    def __init__(self, source_path='', destination_path='', file_type='', rotate=False, task_id=None, config_file=None):
        self.source_path = source_path
        self.destination_path = destination_path
        self.file_type = file_type
        self.rotate = rotate
        self.task_id = task_id

        self.logger = get_logger('azure_app', 'azure_vms.log')
        self.good_subscription_statuses = ['Enabled']
        self.vm_running_states = ['PowerState/running']
        self.good_vm_states = ['PowerState/deallocated']  # 'PowerState/stopped',

        # Getting the idle and cancel keys from redis dbs
        self.redis_cli = redis.Redis(host=settings.IDLE_HOST, port=settings.IDLE_PORT, db=settings.IDLE_DB_NUMBER)
        self.redis_cancel = redis.Redis(host=settings.IDLE_HOST, port=settings.IDLE_PORT, db=settings.CANCEL_DB_NUMBER)

        self.cancel_vms = []
        for key in self.redis_cancel.scan_iter('*'):
            splitted_key = key.decode('utf-8').split('__')
            if len(splitted_key) == 3:
                self.cancel_vms.append(key.decode('utf-8').split('__'))

        self.idle_vms = []
        for key in self.redis_cli.scan_iter('*'):
            splitted_key = key.decode('utf-8').split('__')
            if splitted_key not in self.cancel_vms and len(splitted_key) == 3:
                self.idle_vms.append(splitted_key)

        self.log('Idle VMs: {}'.format(self.idle_vms))
        self.log('Cancel VMs: {}'.format(self.cancel_vms))

        if config_file:
            azure_config_file = os.path.join(settings.BASE_DIR, 'processapp/process_module', config_file)
        else:
            if settings.ENVIRONMENT == 'PROD':
                azure_config_file = os.path.join(settings.BASE_DIR, 'processapp/process_module/azure_config.json')
            else:
                azure_config_file = os.path.join(settings.BASE_DIR, 'processapp/process_module/azure_config_dev.json')

        with open(azure_config_file) as f:
            azure_accounts = json.load(f)
        self.azure_accounts = azure_accounts['AZURE_ACCOUNTS']
        self.CHOSEN_VM = None
        self.compute_client = None

    def _check_subscription(self, credentials, account_name, subscription_id):
        """ Checking the subscription state """
        self.log('  Checking the subscription {} for account {}'.format(subscription_id,
                                                                        account_name))
        subscription_client = SubscriptionClient(credentials)

        curr_subscription = subscription_client.subscriptions.get(subscription_id)
        subscription_info = curr_subscription.state
        self.log('      Got the subscription state')

        available_subscription = True
        if subscription_info not in self.good_subscription_statuses:
            available_subscription = False

        self.log('      Checked the subscription availability')

        return available_subscription, subscription_info

    @staticmethod
    def _get_compute_client(account_info):
        """ Returns the compute client and credentials for a specified account """
        credentials = ServicePrincipalCredentials(client_id=account_info['CLIENT_ID'],
                                                  secret=account_info['CLIENT_SECRET'],
                                                  tenant=account_info['TENANT_ID'])
        compute_client = ComputeManagementClient(credentials, account_info['SUBSCRIPTION_ID'])

        return compute_client, credentials

    def get_available_vms(self):
        """ Get available (not started) VMs from the list """
        start = time()

        vms_count = 0
        vms_info = []

        self.log('Checking the vm count')
        for account_name, account_info in self.azure_accounts.items():
            vm_idle = False

            compute_client, credentials = self._get_compute_client(account_info)

            self.log('  Checking the subscription {} for account {}'.format(account_info['SUBSCRIPTION_ID'],
                                                                            account_name))

            available_subscription, subscription_info = self._check_subscription(credentials,
                                                                                 account_name,
                                                                                 account_info['SUBSCRIPTION_ID'])

            if not available_subscription:
                self.log('      [NOT OK] Subscription {} has the status {}'.format(account_name, subscription_info))
                continue
            else:
                self.log('      Subscription {} has the status {}'.format(account_name, subscription_info))
            self.log('  Done checking the subscription')
            self.log('Checking which VMs are available for account {}'.format(account_name))
            for vm in account_info['VIRTUAL_MACHINES']:
                _async_vm = compute_client.virtual_machines.instance_view(vm['GROUP_NAME'], vm['VM_NAME'])

                vm_status_code = _async_vm.statuses[1].code
                if vm_status_code in self.vm_running_states and \
                        [account_name, vm['GROUP_NAME'], vm['VM_NAME']] in self.cancel_vms:
                    self.log('  Remove from VM {}__{} from cancel'.format(account_name, vm['VM_NAME']))
                    redis_key = '{account}__{group}__{vm}'.format(account=account_name,
                                                                  group=vm['GROUP_NAME'],
                                                                  vm=vm['VM_NAME'])

                    self.redis_cancel.delete(redis_key)
                    self.idle_vms.append(redis_key.split('__'))

                if vm_status_code in self.good_vm_states or \
                        [account_name, vm['GROUP_NAME'], vm['VM_NAME']] in self.idle_vms:
                    self.log('  FOUND: Account: {}, Group Name: {}, VM: {},'.format(account_name,
                                                                                    vm['GROUP_NAME'],
                                                                                    vm['VM_NAME']))
                    vms_count += 1
                    vm_idle = [account_name, vm['GROUP_NAME'], vm['VM_NAME']] in self.idle_vms

                if vm_idle:
                    vm_status = 'Idle'
                else:
                    vm_status = re.search('(:?/)(.+)', _async_vm.statuses[1].code).group(2).title()
                vms_info.append({'account_name': account_name,
                                 'sub_info': subscription_info.value,
                                 'group_name': vm['GROUP_NAME'],
                                 'vm_nickname': vm['VM_NICKNAME'],
                                 'vm_name': vm['VM_NAME'],
                                 'vm_status': vm_status
                                 })
            self.log('  Total VMs available: {}'.format(vms_count))

        self.log('CHECKING VMS TIME: {}'.format(time() - start))
        return vms_count, vms_info

    def _get_available_vm(self):
        """ Get available (not started, starting or deallocating) VMs from the list """
        self.log('Getting available VMs')

        if self.idle_vms:  # If there is a VM in idle, choose that one
            self.log('Found an IDLE VM')
            account_name, vm_group, vm_name = self.idle_vms[0]
            account_info = self.azure_accounts[account_name]

            vm = None
            for vm in account_info['VIRTUAL_MACHINES']:
                # get all the info about the VM
                if vm['GROUP_NAME'] == vm_group and vm['VM_NAME'] == vm_name:
                    vm = vm
                    break

            self.log('  Got the IDLE VM {}: {}'.format(account_name, vm['VM_NAME']))

            compute_client, _ = self._get_compute_client(account_info)

            _async_vm = compute_client.virtual_machines.instance_view(vm['GROUP_NAME'], vm['VM_NAME'])
            self.compute_client = compute_client

            vm['AZURE_ACCOUNT_NAME'] = account_name
            self.log('  SELECTED: Account: {}, Group Name: {}, VM: {},'.format(account_name,
                                                                               vm['GROUP_NAME'],
                                                                               vm['VM_NAME']))
            return vm

        for account_name, account_info in self.azure_accounts.items():
            compute_client, credentials = self._get_compute_client(account_info)

            self.log('  Checking the subscription {} for account {}'.format(account_info['SUBSCRIPTION_ID'],
                                                                            account_name))

            available_subscription, subscription_info = self._check_subscription(credentials,
                                                                                 account_name,
                                                                                 account_info['SUBSCRIPTION_ID'])

            if not available_subscription:
                self.log('      [NOT OK] Subscription {} has the status {}'.format(account_name, subscription_info))
                continue
            else:
                self.log('      Subscription {} has the status {}'.format(account_name, subscription_info))

            for vm in account_info['VIRTUAL_MACHINES']:
                _async_vm = compute_client.virtual_machines.instance_view(vm['GROUP_NAME'], vm['VM_NAME'])

                if _async_vm.statuses[1].code in self.good_vm_states:
                    self.compute_client = compute_client
                    self.log('  SELECTED: Account: {}, Group Name: {}, VM: {},'.format(account_name,
                                                                                       vm['GROUP_NAME'],
                                                                                       vm['VM_NAME']))
                    vm['AZURE_ACCOUNT_NAME'] = account_name
                    return vm
        return None

    def _prepare_data(self, file, destination, file_type, rotate, task_id=None):
        """ Prepare the uploaded file and send it to the chosen VM """
        # Make the config file
        self.log('PREPARING AND SENDING THE FILE')
        self.log('  Building the config file')
        config = {'server_ip': settings.SERVER_IP,
                  'server_user': settings.SERVER_USER,
                  'file': file,
                  'destination': destination,
                  'type': file_type,
                  'rotate': rotate,
                  }
        if task_id:
            config['server_url'] = '{server_url}/ws/progress/{task_id}'.format(server_url=settings.SERVER_URL,
                                                                               task_id=task_id)

        self.log('  Config:\n{}'.format(config))
        file_name, _ = os.path.splitext(os.path.basename(config['file']))
        directory, _ = os.path.splitext(config['file'])

        if not os.path.exists(directory):
            os.mkdir(directory)

        config_file_name = 'config.json'
        config_file = os.path.join(directory, config_file_name)
        with open(config_file, 'w') as f:
            json.dump(config, f)
        self.log('  Finished creating the config file')

        # Copy the file to the data folder
        shutil.copy(config['file'], directory)
        self.log('  File has been copied to config directory')
        # Archive the config and the file with tar
        self.log('  Started building the archive')
        archive_name = os.path.splitext(os.path.basename(config['file']))[0] + '.tar.gz'
        p = subprocess.Popen(['tar', '-zcvf', archive_name, os.path.basename(config['file']), config_file_name],
                             cwd=directory)
        p.wait()
        self.log('  Done.')
        return directory, archive_name

    @staticmethod
    def _scp_files(chosen_vm, directory, archive_name):
        subprocess.check_output(
            ['sshpass', '-p', chosen_vm['VM_PASSWORD'], 'scp', os.path.join(directory, archive_name),
             '{username}@{ip}:{path}'.format(username=chosen_vm['VM_USERNAME'],
                                             ip=chosen_vm['VM_IP'],
                                             path='/home/{username}/media'
                                             .format(username=chosen_vm['VM_USERNAME'])
                                             )
             ])

    def _send_data(self, chosen_vm, directory, archive_name):
        """ Send archive to the VM """
        self.log('  Sending the archive over SCP')
        self.log(' '.join(['sshpass', '-p', chosen_vm['VM_PASSWORD'], 'scp', os.path.join(directory, archive_name),
                           '{username}@{ip}:{path}'.format(username=chosen_vm['VM_USERNAME'],
                                                           ip=chosen_vm['VM_IP'],
                                                           path='/home/{username}/media'
                                                           .format(username=chosen_vm['VM_USERNAME'])
                                                           )
                           ]))

        retries = 0
        while retries < 10:
            try:
                self._scp_files(chosen_vm, directory, archive_name)
                break
            except Exception as e:
                self.log('ERROR SCP: {}, retries={}'.format(repr(e), retries))
                sleep(5)
                retries += 1

        self.log('  Finished sending the file')

        # Delete the folder
        try:
            shutil.rmtree(directory)
        except Exception as e:
            self.log('  !!!!ERROR WHILE DELETING THE DIRECTORY\n\n{}'.format(repr(e)), time=True)

    def start_vm(self, chosen_vm=None):
        """ Start virtual machine and copy the uploaded file with its config file to the machine

        Start the VM or remove it from IDLE (whichever applies), preparing the data and sending it to the VM

        :param chosen_vm -> optional argument specifying a VM
        :returns list: chosen vm

        """
        self.log('SELECTING AND STARTING A VM')
        try:
            if chosen_vm:
                CHOSEN_VM = chosen_vm
            else:
                CHOSEN_VM = self._get_available_vm()
            self.log('Chosen VM: {}'.format(CHOSEN_VM))
            if not CHOSEN_VM:
                self.log('  !!!No VM, try again')
                return []

            redis_key = '{account}__{group}__{vm}'.format(account=CHOSEN_VM['AZURE_ACCOUNT_NAME'],
                                                          group=CHOSEN_VM['GROUP_NAME'],
                                                          vm=CHOSEN_VM['VM_NAME'])

            redis_value = self.redis_cli.get(redis_key)
            vm_idle = True
            async_vm = None

            if redis_value:
                self.log('\nVM is IDLE')
                self.redis_cli.delete(redis_key)
                self.log('VM removed from IDLE')
            else:
                self.log('\nStart VM <=================', time=True)
                async_vm = self.compute_client.virtual_machines.start(CHOSEN_VM['GROUP_NAME'], CHOSEN_VM['VM_NAME'])
                vm_idle = False

            ws_send_next_stage(self.task_id, 'loading', 55.31)

            try:
                self.log('Preparing the data')
                archive_directory, archive_name = self._prepare_data(self.source_path, os.path.dirname(self.source_path),
                                                                     self.file_type, self.rotate, self.task_id)
                self.log('Done. Dir: {}, Arch: {}'.format(archive_directory, archive_name))
            except Exception as e:
                self.log('EXCEPTION WHILE PREPARING THE DATA: {}'.format(repr(e)))
                raise e

            ws_send_next_stage(self.task_id, 'loading', 62.89)

            if not vm_idle:
                async_vm.wait()
                # sleep(40)  # sleep until the ssh on the VM works
                self.log('=================>\nVM started', time=True)

            self.log('  VM <{} ({})> Successfully Started'.format(CHOSEN_VM['VM_NAME'], CHOSEN_VM['GROUP_NAME']))
            self._send_data(CHOSEN_VM, archive_directory, archive_name)

            ws_send_next_stage(self.task_id, 'loading', 89.52)

            self.log('  DATA PREPARED AND SENT')

            self.log('Done.')
        except Exception as e:
            self.log('ERROR IN STARTING VM')
            self.log(repr(e))
            raise e
        return CHOSEN_VM, vm_idle

    def stop_vm(self, vm, group_name='', azure_account_name='', wait=False):
        """ Stop virtual machine"""
        self.log('\nSTOPING THE VM')
        with open('file', 'w') as f:
            f.write('{} {} {}'.format(vm, group_name, azure_account_name))
        if not group_name or not azure_account_name:
            self.log('  Normal shutdown (NO REDIS)')
            self.log('  SHUTTING DOWN VM {} FROM GROUP {}'.format(vm['GROUP_NAME'], vm['VM_NAME']))
            async_vm = self.compute_client.virtual_machines.deallocate(vm['GROUP_NAME'], vm['VM_NAME'])
            if wait:
                async_vm.wait()
            self.log('Done', time=True)
            return

        if type(vm) == type(dict):
            self.log('  WARNING!!! VM must be a string when using ELICIT SHUTDOWN')
            vm = vm['VM_NAME']

        self.log('  Explicit shutdown (WITH REDIS)')
        self.log('  Shutting down: Account: {}, Group: {}, VM: {}'.format(azure_account_name,
                                                                          group_name,
                                                                          vm))
        account_info = self.azure_accounts[azure_account_name]
        credentials = ServicePrincipalCredentials(client_id=account_info['CLIENT_ID'],
                                                  secret=account_info['CLIENT_SECRET'],
                                                  tenant=account_info['TENANT_ID'])

        compute_client = ComputeManagementClient(credentials, account_info['SUBSCRIPTION_ID'])
        async_vm = compute_client.virtual_machines.deallocate(group_name, vm)
        if wait:
            async_vm.wait()
        self.log('Done', time=True)

    def log(self, s, time=None):
        self.logger.info(s)

