"""

@Created 2019-01-17
@author: Andrei Damian
@opyright: Lummetry.AI

@modified:
   2019-01-17 created

"""
import os

from azure.mgmt.compute import ComputeManagementClient
from azure.common.credentials import ServicePrincipalCredentials

from azure.mgmt.billing import BillingManagementClient
from azure.mgmt.subscription import SubscriptionClient

from django.conf import settings

from .logger_helper import LoadLogger


class AzureSubscriptionDebugger:
    def __init__(self,
                 SUBSCRIPTION_ID=None,
                 AZURE_CLIENT_ID=None,
                 AZURE_CLIENT_SECRET=None,
                 AZURE_TENANT_ID=None,
                 log=None):
        if log is None:
            log = print
            # log = LoadLogger('AZRH', os.path.join(settings.BASE_DIR, 'processapp/process_module/config.txt'), load_tf=False)
        self.log = log

        if SUBSCRIPTION_ID is None:
            SUBSCRIPTION_ID = self.log.config_data['AZURE_SUBSCRIPTION_ID']
        if AZURE_CLIENT_ID is None:
            AZURE_CLIENT_ID = self.log.config_data['AZURE_CLIENT_ID']
        if AZURE_CLIENT_SECRET is None:
            AZURE_CLIENT_SECRET = self.log.config_data['AZURE_CLIENT_SECRET']
        if AZURE_TENANT_ID is None:
            AZURE_TENANT_ID = self.log.config_data['AZURE_TENANT_ID']

        self.good_statuses = ['Enabled']

        self.AZURE_CLIENT_ID = AZURE_CLIENT_ID
        self.AZURE_CLIENT_SECRET = AZURE_CLIENT_SECRET
        self.AZURE_TENANT_ID = AZURE_TENANT_ID
        self.SUBSCRIPTION_ID = SUBSCRIPTION_ID
        self.setup_client()

        return

    def P(self, s, t=False):
        # self.log.P(s, show_time=t)
        self.log(s)
        return

    def setup_client(self):
        subscription_id = self.SUBSCRIPTION_ID
        credentials = ServicePrincipalCredentials(
            client_id=self.AZURE_CLIENT_ID,
            secret=self.AZURE_CLIENT_SECRET,
            tenant=self.AZURE_TENANT_ID
        )
        self.compute_client = ComputeManagementClient(credentials,
                                                      subscription_id)

        self.billing_client = BillingManagementClient(credentials,
                                                      subscription_id)

        self.subscription_client = SubscriptionClient(credentials,
                                                      base_url=None)

        self.curr_subscription = self.subscription_client.subscriptions.get(subscription_id)
        self.curr_subscription_state = self.curr_subscription.state

        self.P("Connected to subscription [{}]".format(subscription_id))
        self.P(" Subscription status: {}".format(self.curr_subscription_state))
        if self.curr_subscription_state not in self.good_statuses:
            self.P("\nATTENTION: SUBSCRIPTION PROBLEM!!!\n")
        return

    def check_subscription(self):
        _res = True
        if self.curr_subscription_state not in self.good_statuses:
            _res = False
        return _res, self.curr_subscription_state

    def debug_vm(self, VM_NAME, GROUP_NAME=None):
        if GROUP_NAME is None:
            GROUP_NAME = self.log.config_data['AZURE_GROUP_NAME']
        _vm_type, _res = self._degub_vm_info(self, GROUP_NAME, VM_NAME)
        return _vm_type, _res

    def _degub_vm_info(self, GROUP_NAME, VM_NAME):
        """
         compute_client: assumes that compute_client object is initialized
         GROUP_NAME: name of the resrouce group
         VM_NAME: name of the vm
        """
        vm = self.compute_client.virtual_machines.get(GROUP_NAME, VM_NAME, expand='instanceView')
        self.P("AzureDebugInfo:")
        self.P(" Subscr: {}".format(self.SUBSCRIPTION_ID))
        self.P(" VM:     {}".format(VM_NAME))
        vm_type = vm.hardware_profile.vm_size
        self.P("   vmSize: {}".format(vm_type))
        self.P(" storageProfile")
        self.P("  imageReference")
        self.P("    publisher: {}".format(vm.storage_profile.image_reference.publisher))
        self.P("    offer: {}".format(vm.storage_profile.image_reference.offer))
        self.P("    sku: {}".format(vm.storage_profile.image_reference.sku))
        self.P("    version: {}".format(vm.storage_profile.image_reference.version))
        self.P("  osDisk")
        self.P("    osType: {}".format(vm.storage_profile.os_disk.os_type.value))
        self.P("    name: {}".format(vm.storage_profile.os_disk.name))
        self.P("    createOption: {}".format(vm.storage_profile.os_disk.create_option))
        self.P("    caching: {}".format(vm.storage_profile.os_disk.caching.value))
        self.P(" osProfile")
        self.P("  computerName: {}".format(vm.os_profile.computer_name))
        self.P("  adminUsername: {}".format(vm.os_profile.admin_username))
        if vm.os_profile.windows_configuration is not None:
            self.P("  provisionVMAgent: {0}".format(vm.os_profile.windows_configuration.provision_vm_agent))
            self.P("  enableAutomaticUpdates: {0}".format(vm.os_profile.windows_configuration.enable_automatic_updates))
        self.P(" networkProfile")
        for nic in vm.network_profile.network_interfaces:
            self.P("  networkInterface id: ...{}".format(nic.id[-30:]))
        if vm.instance_view.vm_agent is not None:
            self.P(" vmAgent")
            self.P("  vmAgentVersion {}".format(vm.instance_view.vm_agent.vm_agent_version))
            self.P("    statuses")
            for stat in vm.instance_view.vm_agent.statuses:
                self.P("    code: {}".format(stat.code))
                self.P("    displayStatus: {}".format(stat.display_status))
                self.P("    message: {}".format(stat.message))
                self.P("    time: {}".format(stat.time))
        self.P(" disks");
        for disk in vm.instance_view.disks:
            self.P("  name: {}".format(disk.name))
            self.P("  statuses")
            for stat in disk.statuses:
                self.P("    code: {}".format(stat.code))
                self.P("    displayStatus: {}".format(stat.display_status))
                self.P("    time: {}".format(stat.time))
        self.P(" VM general status")
        self.P("  provisioningStatus: {}".format(vm.provisioning_state))
        self.P("  id: ...{}".format(vm.id[-30:]))
        self.P("  name: {}".format(vm.name))
        self.P("  type: {}".format(vm.type))
        self.P("  location: {}".format(vm.location))
        self.P(" VM instance status")
        for stat in vm.instance_view.statuses:
            self.P("  code: {}".format(stat.code))
            self.P("  displayStatus: {}".format(stat.display_status))
        return vm_type, vm.instance_view.statuses[-1].code


if __name__ == '__main__':
    VM_NAME = "cfodworker-gpu"
    azhelp = AzureSubscriptionDebugger()
    vm_type, vm_status = azhelp.debug_vm(VM_NAME)
    subs_status, subs_info = azhelp.check_subscription()
    print("\n\n Final results:")
    print("  Subscription ok: {}".format(subs_status))
    print("  VM [{}] Status: {}".format(vm_type,vm_status))
    print("   and {}".format("available" if subs_status else "NOT available due to subscription [{}]".format(subs_info)))