# -*- coding: utf8 -*-
"""
 * 日志流处理服务：
 *  log_message(name, message)
 *
 *  process_local_log_stream(name, callback)
 *
 *  process_remote_log_stream(name, callback, host, pwd=None, user="root", port=22)
 *
 *  WARNING: 虽然每台服务器上打印的日志是严格按顺序的保存和处理的，但是多台服务器之间的日志顺序是无法保证先后顺序的严格一致的
 *
 * Create by Truan Wang on 2021/08/26
 *
 * Copyright ? 2014-2030 . 上海进馨网络科技有限公司 . All Rights Reserved
"""
import os
import sys
import time
import datetime
import logging
import subprocess
try:
    import paramiko
except ImportError:
    paramiko = None

LOG_FILE_SUFIX = ".lsps"
LOGGER_DIR = "/tmp/lsps" if os.name == "posix" else "."
if not os.path.exists(LOGGER_DIR):
    os.makedirs(LOGGER_DIR)


class LSPSRotatingFileHandler(logging.FileHandler):

    def __init__(self, alias, basedir):
        self._basedir = basedir
        self._alias = alias

        self.baseFilename = self._create_base_filename()

        logging.FileHandler.__init__(self, self.baseFilename, "a", "utf-8", True)

    def emit(self, record):
        if self._should_rotate():
            self._do_rotate()

        try:
            logging.FileHandler.emit(self, record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def _create_base_filename(self):
        self._createAt = datetime.datetime.now()
        basename_ = self._alias + "." + self._createAt.strftime("%Y-%m-%d") + LOG_FILE_SUFIX
        return os.path.join(self._basedir, basename_)

    def _should_rotate(self):
        if datetime.datetime.now() - self._createAt > datetime.timedelta(days=1):
            self.baseFilename = self._create_base_filename()
            return True

    def _do_rotate(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None

        self.baseFilename = self._create_base_filename()


def get_logger(name, level=logging.INFO):
    logger = logging.getLogger("lsps-" + name)
    if logger.handlers.__len__() == 0:
        logger.propagate = 0
        logger.setLevel(level)
        folder = os.path.join(LOGGER_DIR, name)
        if not os.path.exists(folder):
            os.makedirs(folder)
        log_handler = LSPSRotatingFileHandler(name, folder)
        log_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(log_handler)

    return logger


def log_message(name, message):
    logger = get_logger(name)
    logger.info(message)


def process_local_log_stream(name, callback, omit_error=False, max_lines=5000):
    assert callable(callback), "callback is not a function like object"
    folder = os.path.join(LOGGER_DIR, name)
    lsps_logger = get_logger("lsps", level=logging.DEBUG if sys.gettrace() else logging.INFO)
    lines_read = 0
    for f in os.listdir(folder):
        if lines_read >= max_lines:
            break
        if f.endswith(LOG_FILE_SUFIX):
            position = 0
            count_file = os.path.join(folder, "." + f + ".counter")
            if os.path.exists(count_file):
                with open(count_file, "r") as cfp:
                    _p = cfp.read()
                    if _p.isdigit():
                        position = int(_p)
            log_file = os.path.join(folder, f)
            lsps_logger.info("%s: start from position %d", log_file, position)
            with open(log_file, "r") as fp:
                if position != 0:
                    fp.seek(position)
                line = fp.readline(1024 * 64)   # read at most 64 KB per line
                position = fp.tell()
                if not line:
                    lsps_logger.debug("%s: have NOT new line", log_file)
                    if time.time() - os.stat(log_file).st_ctime > 3600 * 24 * 1:
                        lsps_logger.info("%s: DELETED", log_file)
                        # 如果 f 的最后修改时间在 1 天前，可以删除文件了。
                        os.unlink(log_file)
                        os.unlink(count_file)
                else:
                    while line:
                        lines_read += 1
                        lsps_logger.debug("%s have new lines", log_file)
                        try:
                            callback(line)
                        except Exception as e:
                            lsps_logger.exception("%s: callback ERROR", log_file)
                            if not omit_error:
                                # save position to file
                                with open(count_file, "w") as cfp:
                                    cfp.write(str(position))
                                raise e   # will not save position

                        if lines_read > max_lines:
                            break
                        line = fp.readline(1024 * 64)  # read at most 64 KB per line
                        position = fp.tell()

                    # save position to file
                    with open(count_file, "w") as cfp:
                        cfp.write(str(position))


def process_remote_log_stream(name, callback, host, pwd=None, user="root", port=22, lsps_path=__file__):
    assert callable(callback), "callback is not a function like object"
    lsps_logger = get_logger("lsps", level=logging.DEBUG if sys.gettrace() else logging.INFO)

    lsps_logger.info("process %s at %s", name, host)

    if paramiko is not None:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, port=port, username=user, password=pwd, timeout=5)
        _, out, err = ssh.exec_command("python %s %s" % (lsps_path, name))
        errors = err.read()
        lines = out.readlines()
        if errors:
            print (errors)
        try:
            callback(lines)
        except Exception:
            lsps_logger.exception("process %s at %s error", name, host)
        finally:
            ssh.close()
    else:
        args = [
            'ssh',  # '-o', '"UserKnownHostsFile=/dev/null"',
            '-o', '"StrictHostKeyChecking=no"',
            '-p', str(port),
            '%s@%s' % (user, host),
            'python', '-S', lsps_path, name
        ]
        if pwd is not None:
            args = ['sshpass', '-p', pwd] + args

        ssh = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        errors = ssh.stderr.read()
        if errors:
            lsps_logger.error("process %s at %s remote error %s", name, host, ssh.stderr.read())

        try:
            callback(ssh.stdout)
        except Exception:
            lsps_logger.exception("process %s at %s error", name, host)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Log Stream Process Services, print local log stream to stdout, for next pipeline process.')
    parser.add_argument(
        'names', metavar='name', nargs='+', type=str,
        help='log stream names to be processed')
    parser.add_argument(
        '--max-lines', dest='max_lines', default=1000, type=int,
        help='limit at most max-lines log can be processed')
    parser.add_argument('--debug', default=False, action='store_true', help='create debug stream')
    args = parser.parse_args()

    def print_to_stdout(lines):
        for line in lines:
            sys.stdout.write(line)

    if args.debug:
        #while True:
        #    for arg in args.names:
        #        process_remote_log_stream(arg, print_to_stdout, "47.97.63.113", lsps_path='/opt/justing/jct/lsps.py')
        #    time.sleep(3)

        i = 0
        while True:
            for arg in args.names:
                log_message(arg, '{"i":%d,"type_":"test","pid":%d,"ts":%d}' % (i, os.getpid(), time.time()))
                time.sleep(1)
                i += 1

    else:
        for arg in args.names:
            process_local_log_stream(arg, print_to_stdout, omit_error=True, max_lines=args.max_lines)
