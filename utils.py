import threading
import subprocess

def getFirstOrDefault(list, key, target):
    return next((n for n in list if n[key] == target))

def open_thread_with_callback(on_exit, on_exit_args, popen_args):
    def run_in_thread(on_exit, popen_args):
        proc = subprocess.Popen(*popen_args)
        proc.wait()
        on_exit(on_exit_args)
        return
    thread = threading.Thread(target=run_in_thread, args=(on_exit, popen_args))
    thread.start()
    return thread