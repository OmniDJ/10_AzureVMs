import cv2
import json
import re
import numpy as np
import subprocess
import redis
import asyncio
import websockets

from datetime import datetime
from functools import partial
from itertools import chain
from magic import from_file as mime_from_file
from os import listdir, makedirs, mkdir, path, remove
from pytz import timezone
from shutil import move, rmtree, copyfile
from time import sleep, time

from celery import shared_task, Task
from django.conf import settings

from processapp.process_utils import ws_send_next_stage
from processapp.process_module.azure_vms import AzureVM
from processapp.process_module.cfod import start_process_video, start_process_image
from processapp.process_module.tracker import MotionTracker
from processapp.process_module.vapor_engine.vapor_helper import VAPORHelper

from .models import UploadModel


class VaporTaskBase(Task):
    """ Make this a base class of a task to have access to callbacks
     based on the task state
    """

    def on_success(self, retval, task_id, args, kwargs):
        """ Callback on celery task finished successfully """
        ws_send_next_stage(task_id, 'success', 100.0)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """ Callback on celery task failed """

        print('TASK {} FAILED'.format(task_id))
        print('exc: {}'.format(exc))
        print('args: {}'.format(args))
        print('kwargs: {}'.format(kwargs))
        print('einfo: {}'.format(einfo))

        # Expected args order: file_path, file_type, computed_eta, rotate, gdpr
        file_path = args[0]
        computed_eta = args[2]

        UploadModel.objects.filter(original_file=file_path).update(process_state=UploadModel.ERROR,
                                                                   computed_eta=computed_eta,
                                                                   end_date=datetime.now(timezone(settings.TIME_ZONE)))
        # Sleep is here because if the task fails immediately after it starts,
        # the UI doesn't have time to load the web socket and the message will have no effect
        sleep(2)
        ws_send_next_stage(task_id, 'failed', 100.0)


def analysis_callback(task_id, file, prc):
    """ Callback for analysis step of vapor engine """
    ws_send_next_stage(task_id, 'analysing', prc, step_no=2)


def process_callback(task_id, file, prc):
    """ Callback for processing step of vapor engine """
    ws_send_next_stage(task_id, 'processing', prc, step_no=3)


@shared_task(bind=True, base=VaporTaskBase)
def start_gpu_annotation_process(self, file_path, file_type, computed_eta, rotate=False, gdpr=True):
    """ Run VAPOR engine on a Azure VM machine """
    start = time()
    print('Start GPU task')
    print('File {}'.format(file_path))

    task_id = self.request.id

    file_dst_path = path.join('results', *file_path.strip('/').split('/')[1:])
    media_src_path = path.join(settings.MEDIA_ROOT, file_path)
    media_src_folder = path.dirname(media_src_path)
    media_dst_path = path.join(settings.MEDIA_ROOT, file_dst_path)
    media_dst_folder = path.dirname(media_dst_path)
    media_res_path = path.join(settings.MEDIA_ROOT, 'results')

    file_name = path.basename(file_path)

    if file_type == 'image':
        self.start_cpu_annotation_process(file_path, file_type, computed_eta, rotate=rotate)
    elif file_type != 'video':
        # Raise for any file other than image and video
        raise ValueError('File should be video or image, not {}'.format(file_type))

    ws_send_next_stage(task_id, 'loading', 12.1)

    r = redis.Redis(host=settings.IDLE_HOST, port=settings.IDLE_PORT, db=settings.IDLE_DB_NUMBER)
    idle_expire_time = settings.IDLE_EXPIRE_TIME

    if not path.exists(media_res_path):
        makedirs(media_res_path)

    # Get available Azure VMs
    azure_vm = AzureVM(media_src_path, media_res_path, file_type, rotate, task_id=task_id)
    chosen_vm = azure_vm._get_available_vm()

    retries = 0
    while not chosen_vm and retries < 30:
        sleep(10)
        chosen_vm = azure_vm._get_available_vm()

    ws_send_next_stage(task_id, 'loading', 41.62)

    redis_key = '{account}__{group}__{vm}'.format(account=chosen_vm['AZURE_ACCOUNT_NAME'],
                                                  group=chosen_vm['GROUP_NAME'],
                                                  vm=chosen_vm['VM_NAME'])

    vm_info_string = '{user}__{ip}__{passwd}__{redis_key}'.format(user=chosen_vm['VM_USERNAME'],
                                                                  ip=chosen_vm['VM_IP'],
                                                                  passwd=chosen_vm['VM_PASSWORD'],
                                                                  redis_key=redis_key)
    UploadModel.objects.filter(original_file=file_path).update(vm_info_string=vm_info_string)
    
    # Start Azure VM
    chosen_vm, vm_idle = azure_vm.start_vm(chosen_vm)

    ws_send_next_stage(task_id, 'loading', 98.37)

    print('         FINISHED: {}'.format(chosen_vm))
    if chosen_vm:
        try:
            archive_name = make_targz(file_name)
            analysis_file = path.join(media_src_folder, make_txt(file_name))

            if not path.exists(media_dst_folder):
                makedirs(media_dst_folder)

            if not vm_idle:
                sleep(int(computed_eta * 0.15))
            retries = 0
            while archive_name not in listdir(media_src_folder) and retries < 360:
                sleep(10)
                print('Archive {} not found, rechecking in 10 seconds'.format(archive_name))
                retries += 1

            print('Moving the VM to IDLE state with key {}'.format(redis_key))
            r.set(redis_key, 'not important', ex=idle_expire_time)
            # azure_vm.stop_vm(chosen_vm)

            if archive_name not in listdir(media_src_folder):
                raise FileNotFoundError('Archive {} not found!'.format(archive_name))

            print('Make results visible in the app...')

            print('Copying from {} to {}'.format(media_src_path, media_dst_path))
            try:
                copyfile(media_src_path, media_dst_path)
            except Exception as e:
                print('Exception while copying the original file: {}'.format(repr(e)))

            print('Dearchiving into the previously created folder. Archive: {}'.format(archive_name))
            subprocess.call(['tar', '-xvf', archive_name, '-C', media_src_folder], cwd=media_src_folder)
            print('Done dearchiving the files')
            # Get tracker messages
            print('Getting tracker messages')
            tracker_messages = get_tracker_messages(analysis_file)

            tracker_messages = '\r\n'.join(tracker_messages)[:-3] + '.'
            if tracker_messages.strip() == '.':
                message = 'No persons or faces were detected in this video file'
            else:
                message = tracker_messages

            print('Saving to DB')
            UploadModel.objects.filter(original_file=file_path).update(file=make_mp4(file_path),
                                                                       original_file=file_dst_path,
                                                                       messages=message,
                                                                       computed_eta=computed_eta,
                                                                       end_date=datetime.now(
                                                                           timezone(settings.TIME_ZONE)),
                                                                       process_state=UploadModel.SUCCESSFUL)

            print('Removing temporary files/folders')
            if path.exists(media_src_path) and path.splitext(media_src_path)[1] != '.mp4':
                # Remove original video file if the extension is different than mp4
                try:
                    remove(media_src_path)
                except:
                    print('FAILED TO DELETE THE FILE {}'.format(media_src_path))

            try:
                remove(path.join(media_src_folder, archive_name))
            except:
                print('FAILED TO DELETE THE ARCHIVE {}'.format(path.join(media_src_folder, archive_name)))

            print('finished task')
            end = time()
            print('Task time: {}'.format(end-start))
        except Exception as e:
            print(repr(e))
            r.set(redis_key, 'not important', ex=idle_expire_time)
            raise e
    else:
        print('NO VM FOUND!! ABORT PROCESS!!!')
        raise Exception('NO VM FOUND')




@shared_task(bind=True)
def start_blurring_faces(self, file_path, original_file_path, face_ids, selected_face_ids):
    """ Celery task for blurring specific faces in a media file based on a list of ids """
    start = time()
    print('Start Bluring task')
    print('File {}'.format(original_file_path))

    try:
        face_ids = apply_blur_to_faces(file_path, original_file_path, face_ids, selected_face_ids)
        UploadModel.objects.filter(original_file=original_file_path).update(face_ids=json.dumps(face_ids),
                                                                            process_state=UploadModel.SUCCESSFUL)
    except:
        UploadModel.objects.filter(original_file=original_file_path).update(process_state=UploadModel.ERROR)
    print('finished task')
    end = time()
    print('Task time: {}'.format(end - start))


def apply_blur_to_faces(file_path, original_file_path, face_ids, selected_face_ids):
    """ Apply blur to a given list of face ids """
    media_src_path = path.join(settings.MEDIA_ROOT, original_file_path)
    media_dst_path = path.join(settings.MEDIA_ROOT, file_path)

    print('Getting the file type')
    file_type = mime_from_file(media_src_path, mime=True).split('/')[0]

    print('File type: {}'.format(file_type))
    face_ids = json.loads(face_ids)
    try:
        FRAME_W, FRAME_H = 1280, 720
        if file_type == 'image':
            image = cv2.imread(media_src_path)

            image_faces = face_ids.get('0', dict())

            for face_id in image_faces.keys():
                face_label = image_faces[face_id][2]
                face_tuple = image_faces[face_id][0]

                if face_id in selected_face_ids:
                    resized_h, resized_w = image_faces[face_id][3]
                    image_resized = image[:resized_h, -resized_w:, :]
                    image[:resized_h, -resized_w:, :] = cv2.GaussianBlur(image_resized, (51, 51), 0)

                    image = apply_blur(image, (151, 151), *face_tuple)

                    image = cv2.putText(image, face_label, (face_tuple[0] + 2, face_tuple[1] + 13),
                                        fontFace=cv2.FONT_HERSHEY_PLAIN, fontScale=1.1,
                                        color=(255, 255, 255))

                    face_ids['0'][face_id][1] = True
                else:
                    image = cv2.putText(image, face_label, (face_tuple[0] + 2, face_tuple[1] + 13),
                                        fontFace=cv2.FONT_HERSHEY_PLAIN, fontScale=1.1,
                                        color=(255, 255, 255))
                    face_ids['0'][face_id][1] = False

            cv2.imwrite(media_dst_path, image)
            cv2.destroyAllWindows()
        elif file_type == 'video':
            # media_src_path = make_mp4(media_src_path)
            print('Opening video {}'.format(media_src_path))
            video = cv2.VideoCapture(media_src_path)

            # Get the codec from the video
            fps = video.get(cv2.CAP_PROP_FPS)
            frame_size = (FRAME_W, FRAME_H)
            fourcc = cv2.VideoWriter_fourcc(*'AVC1')

            print('Writing new video at {}'.format(media_dst_path))
            new_video = cv2.VideoWriter(media_dst_path, fourcc, fps, frame_size)
            frame_count = 0
            print('Started bluring of video the file')
            while video.isOpened():
                frame_count += 1
                ret, frame = video.read()
                if ret:
                    frame_faces = face_ids.get(str(frame_count), dict())
                    for face_id in frame_faces.keys():
                        face_label = frame_faces[face_id][2]
                        face_tuple = frame_faces[face_id][0]

                        if face_id in selected_face_ids:
                            resized_h, resized_w = frame_faces[face_id][3]
                            image_resized = frame[:resized_h, -resized_w:, :]
                            frame[:resized_h, -resized_w:, :] = cv2.GaussianBlur(image_resized, (51, 51), 0)

                            frame = apply_blur(frame, (151, 151), *face_tuple)

                            frame = cv2.putText(frame, face_label, (face_tuple[0] + 2, face_tuple[1] + 13),
                                                fontFace=cv2.FONT_HERSHEY_PLAIN, fontScale=1.1,
                                                color=(255, 255, 255))

                            face_ids[str(frame_count)][face_id][1] = True
                        else:
                            frame = cv2.putText(frame, face_label, (face_tuple[0] + 2, face_tuple[1] + 13),
                                                fontFace=cv2.FONT_HERSHEY_PLAIN, fontScale=1.1,
                                                color=(255, 255, 255))
                            face_ids[str(frame_count)][face_id][1] = False
                    new_video.write(frame)
                    if cv2.waitKey(1) & 0xFF == ord('q'):
                        break
                else:
                    break
            video.release()
            new_video.release()
            cv2.destroyAllWindows()
        return face_ids

    except Exception as e:
        print('EXCEPTION inside BLUR Task')
        print(repr(e))
        raise e


def apply_blur(np_img, blur_window, left, top, right, bottom):
    """ Applying blur to a specific region in an image"""
    left = max(0, int(left))
    top = max(0, int(top))
    right = min(np_img.shape[1] - 1, int(right))
    bottom = min(np_img.shape[0] - 1, int(bottom))

    mask = np.zeros(np_img.shape, dtype=np.uint8)
    mask = cv2.rectangle(mask, pt1=(left, top), pt2=(right, bottom), color=(255, 255, 255), thickness=-1)
    blur_np_img = cv2.GaussianBlur(np_img, blur_window, 0)
    np_img = np.where(mask != np.array([255, 255, 255]), np_img, blur_np_img)

    return np_img


def get_tracker_messages(file):
    """ Return tracker messages based on an output file generated by analysis """
    tracker = MotionTracker(min_overlap=0.5, max_overlap=0.85)
    with open(file, 'r') as f:
        analysis_dict = json.load(f)

    frames = analysis_dict['03_FRAMES']
    fps = analysis_dict['01_INFOS']['FPS']
    for idx, frame in enumerate(frames):
        if frame:
            frame_time = idx / max(25, fps) * 1000
            frame_predictions = [(pred['TYPE'], pred['PROB_PRC'], pred['TLBR_POS']) for pred in frame]
            tracker.look_for_changes(frame_predictions, frame_time)
    return tracker.messages


def make_txt(file_path):
    """ Change file path extension to mp4 """
    return path.splitext(file_path)[0] + '.txt'


def make_targz(file_path):
    """ Change file path extension to mp4 """
    return path.splitext(file_path)[0] + '.tar.gz'


def make_mp4(file_path):
    """ Change file path extension to mp4 """
    return path.splitext(file_path)[0] + '.mp4'
