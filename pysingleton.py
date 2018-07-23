####################################################################
#       thread-safe singleton, used by Class inheritance,
#       __init__ methods will only called once
#
import time
import random
import threading
from functools import wraps


class Singleton(object):
    instances = {}
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls in cls.instances:
            return cls.instances[cls]['instance']
        cls._lock.acquire()
        try:
            # double check
            if cls in cls.instances:
                return cls.instances[cls]['instance']
            instance = object.__new__(cls)
            cls.instances[cls] = {
                'instance': instance,
                'is_init': False,
                'cls_lock': threading.Lock()  # each class own a lock for calling __init__
            }
            #  __init__ use cls instead of instance
            setattr(cls, '__init__', cls.__decorate_init(cls.__init__))
        finally:
            cls._lock.release()
        return cls.instances[cls]['instance']

    @classmethod
    def __decorate_init(cls, _init):
        """ __init__ only called once """
        @wraps(_init)
        def wrap_init(*args, **kwargs):
            if Singleton.instances[cls]['is_init']:
                return
            Singleton.instances[cls]['cls_lock'].acquire()
            try:
                # double check
                if Singleton.instances[cls]['is_init']:
                    return
                else:
                    _init(*args, **kwargs)
                    Singleton.instances[cls]['is_init'] = True
            finally:
                Singleton.instances[cls]['cls_lock'].release()

        return wrap_init


###########################################################
#       test function
#
if __name__ == '__main__':
    class F(object):
        def __init__(self):
            self.x = 11
            print('f init once')


    class A(Singleton, F):
        def __init__(self):
            super().__init__()
            self.val = 10
            print(f'hello A init once: {id(self)}')


    class B(A, F):
        def __init__(self):
            super().__init__()
            print('init B once')


    def test():
        obj = B()
        print('thread: ', id(obj), obj.val)
        time.sleep(random.random() / 3)
        obj.val = 5


    ts = []
    for i in range(100):
        t = threading.Thread(target=test)
        ts.append(t)
        t.start()

    for t in ts:
        t.join()
