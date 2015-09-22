#!/usr/bin/python
#coding: UTF-8
# based on seafobj backend of Seafile
import rados
import Queue
import argparse

from ctypes import c_char_p

def ioctx_set_namespace(ioctx, namespace):
    '''Python rados client has no binding for rados_ioctx_set_namespace, we
    add it here.

    '''
    ioctx.require_ioctx_open()
    if isinstance(namespace, unicode):
        namespace = namespace.encode('UTF-8')

    if not isinstance(namespace, str):
        raise TypeError('namespace must be a string')

    rados.run_in_thread(ioctx.librados.rados_ioctx_set_namespace,
                        (ioctx.io, c_char_p(namespace)))


def to_utf8(s):
    if isinstance(s, unicode):
        s = s.encode('utf-8')

    return s


class CephConf(object):
    def __init__(self, ceph_conf_file, pool_name, ceph_client_id):
        self.pool_name = pool_name
        self.ceph_conf_file = ceph_conf_file
        self.ceph_client_id = ceph_client_id


class LibraryStatistics(object):
    def __init__(self, uuid):
        self.uuid = uuid
        self.sizes = {}
        self.objects = {}
        for objtype in ['fs', 'commits', 'blocks', 'sum']:
            self.sizes[objtype] = 0
            self.objects[objtype] = 0

    def add_obj(self, objtype, size):
        if not objtype in ['fs', 'commits', 'blocks']:
            raise ValueError("Invalid object type!")
        self.sizes[objtype] += size
        self.objects[objtype] += 1
        self.sizes['sum'] += size
        self.objects['sum'] += 1

    def __str__(self):
        #return ("uuid=%s,commits_size=%u,commits_obj=%u,fs_size=%u,fs_obj=%u,blocks_size=%u,blocks_obj=%u,size=%u,obj=%u"
        return ("%s,%u,%u,%u,%u,%u,%u,%u,%u"
                % (self.uuid, self.sizes['commits'], self.objects['commits'], self.sizes['fs'], self.objects['fs'],
                   self.sizes['blocks'], self.objects['blocks'], self.sizes['sum'], self.objects['sum']))


class IoCtxPool(object):
    '''since we need to set the namespace before read the object, we need to
    use a different ioctx per thread.

    '''
    def __init__(self, conf, pool_size=5):
        self.conf = conf
        self.pool = Queue.Queue(pool_size)
        if conf.ceph_client_id:
            self.cluster = rados.Rados(conffile=conf.ceph_conf_file, rados_id=conf.ceph_client_id)
        else:
            self.cluster = rados.Rados(conffile=conf.ceph_conf_file)

    def get_ioctx(self, repo_id):
        try:
            ioctx = self.pool.get(False)
        except Queue.Empty:
            ioctx = self.create_ioctx()

        ioctx_set_namespace(ioctx, repo_id)

        return ioctx

    def create_ioctx(self):
        if self.cluster.state != 'connected':
            self.cluster.connect()

        ioctx = self.cluster.open_ioctx(self.conf.pool_name)
        return ioctx

    def return_ioctx(self, ioctx):
        try:
            self.pool.put(ioctx, False)
        except Queue.Full:
            ioctx.close()

class SeafCephClient(object):
    '''Wraps a Ceph ioctx'''
    def __init__(self, conf):
        self.ioctx_pool = IoCtxPool(conf)

    def read_object_content(self, repo_id, obj_id):
        repo_id = to_utf8(repo_id)
        obj_id = to_utf8(obj_id)

        ioctx = self.ioctx_pool.get_ioctx(repo_id)

        try:
            stat = ioctx.stat(obj_id)
            return ioctx.read(obj_id, length=stat[0])
        finally:
            self.ioctx_pool.return_ioctx(ioctx)


def main():
    # parse command line
    cmd_parser = argparse.ArgumentParser(
        description="get seafile usage statistics from ceph"
    )
    cmd_parser.add_argument("--client","--id","-i", dest="client", default='seafile-test',
                            help="ceph client id")
    cmd_parser.add_argument("--blocks-pool","-b", dest="blocks", default='seafile-test-blocks',
                            help="ceph pool containing block objects")
    cmd_parser.add_argument("--commits-pool","-c", dest="commits", default='seafile-test-commits',
                            help="ceph pool containing commit objects")
    cmd_parser.add_argument("--fs-pool","-f", dest="fs", default='seafile-test-commits',
                            help="ceph pool containing fs objects")
    cmd_parser.add_argument("--config", dest="config", default='/etc/ceph/ceph.conf',
                            help="ceph.conf")
    cmd_parser.add_argument('-V', '--verbose', action="store_true",
                            dest="verbose",
                            help="Give detailed information, if possible")
    args = cmd_parser.parse_args()

    libraries = {}

    conf = CephConf(args.config, args.commits, args.client)
    pool = IoCtxPool(conf)

    # get objects of all namespaces for every pool
    for objtype, poolname in {'commits': args.commits, 'fs': args.fs, 'blocks': args.blocks}.items():
        pool.conf.pool_name = poolname
        ioctx = pool.create_ioctx()
        ioctx_set_namespace(ioctx, '\001')
        for obj in ioctx.list_objects():
            if not obj.nspace in libraries:
                libraries[obj.nspace] = LibraryStatistics(obj.nspace)
            
            ioctx_set_namespace(ioctx, obj.nspace)
            libraries[obj.nspace].add_obj(objtype, ioctx.stat(obj.key)[0])
        ioctx.close()
        
    # output results
    print "uuid,commits_size,commits_obj,fs_size,fs_obj,blocks_size,blocks_obj,size,obj"
    for lib in libraries.values():
        print lib
 

if __name__ == '__main__':
    main()
