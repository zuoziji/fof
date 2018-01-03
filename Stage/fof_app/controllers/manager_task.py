from flask import render_template, Blueprint,jsonify,request
from flask_login import current_user
from config_fh import get_redis
from ..extensions import SocketIo as socketio,cache
from flask_socketio import emit
from os import path
import json
from celery import current_app
from ..tasks import stress_testing, weekly_task, daily_mid_night_task, daily_task, daily_night_task
from datetime import datetime
from time import time
import kombu.five
import logging
logger = logging.getLogger()
task_blueprint = Blueprint('task',__name__,template_folder=path.join(path.pardir,'templates','task'))


thread = None
async_mode = None



@task_blueprint.route('/')
def index():
    logger.info("{}访问task管理界面".format(current_user.username))
    fof_list = cache.get(str(current_user.id))
    all_task = current_app.tasks.keys()
    normal_task = [ i for i in all_task if '.' not in i]
    normal_task = [{"id":index,"text":i} for index,i in enumerate(normal_task)]

    return render_template('show_task.html',async_mode=async_mode,fof_list=fof_list,normal_task=normal_task)


@task_blueprint.route('/current',methods=['POST','GET'])
def current():
    inspect = current_app.control.inspect()
    active_dict = inspect.active()
    logger.info("当前正在运行的任务{}".format(active_dict))
    if active_dict is not None:
        active_task = [{"name":i['name'],'id':i['id'],"time_start":datetime.fromtimestamp(time() - (kombu.five.monotonic() - i['time_start'])).strftime('%Y-%m-%d %H:%M:%S')
                        } for _,v in active_dict.items() for i in v]
        return jsonify(status='ok',task=active_task)
    else:
        return jsonify(status='ok',task=0)


@task_blueprint.route('/manual_execute',methods=['POST','GET'])
def manual_execute():
    name = request.json['name']
    print(name)
    if name == 'stress_testing':
        stress_testing.apply_async()
    elif name == "daily_night_task":
        daily_night_task.apply_async()
    elif name == "daily_mid_night_task":
        daily_mid_night_task.apply_async()
    elif name == "daily_task":
        daily_task.apply_async()
    elif name == "weekly_task":
        weekly_task.apply_async()
    return jsonify(status='ok')


@socketio.on('connect', namespace='/stream')
def connect():
    global thread
    if thread is None:
        thread = socketio.start_background_task(target=send_log)
    emit('my_response', {'data':'connect','states':'connect'})


@socketio.on('disconnect', namespace='/stream')
def disconnect():
    print('Client disconnected')


def send_log():
    r = get_redis(host='10.0.5.107', db=5)
    t = r.pubsub()
    t.subscribe('stream')
    while True:
        for item in t.listen():
            log = item['data']
            if log != 1:
                try:
                    log = log.decode('utf-8')
                    socketio.emit('my_response',
                                  {'log':json.loads(log),'states':'connect'},
                                  namespace='/stream')
                except TypeError:
                    print(log)


