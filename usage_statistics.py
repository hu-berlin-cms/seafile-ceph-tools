#!/usr/bin/python
#coding: UTF-8
import rados
import Queue
import argparse
import os

os.environ['SEAFILE_CONF_DIR'] = "."

from seafobj.utils.ceph_utils import ioctx_set_namespace
from seafobj.backends.ceph import CephConf, IoCtxPool

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
        return ("%s,%u,%u,%u,%u,%u,%u,%u,%u"
                % (self.uuid, self.sizes['commits'], self.objects['commits'],
                   self.sizes['fs'], self.objects['fs'],
                   self.sizes['blocks'], self.objects['blocks'],
                   self.sizes['sum'], self.objects['sum']))


def main():
    # parse command line
    cmd_parser = argparse.ArgumentParser(
        description="get seafile usage statistics from ceph"
    )
    cmd_parser.add_argument("--client", "--id", "-i", dest="client",
                            default='seafile', help="ceph client id")
    cmd_parser.add_argument("--blocks-pool", "-b", dest="blocks",
                            default='seafile-blocks',
                            help="ceph pool containing block objects")
    cmd_parser.add_argument("--commits-pool", "-c", dest="commits",
                            default='seafile-commits',
                            help="ceph pool containing commit objects")
    cmd_parser.add_argument("--fs-pool", "-f", dest="fs",
                            default='seafile-commits',
                            help="ceph pool containing fs objects")
    cmd_parser.add_argument("--config", dest="config",
                            default='/etc/ceph/ceph.conf',
                            help="ceph.conf")
    cmd_parser.add_argument('-V', '--verbose', action="store_true",
                            dest="verbose",
                            help="Give detailed information, if possible")
    args = cmd_parser.parse_args()

    libraries = {}

    conf = CephConf(args.config, args.commits, args.client)
    pool = IoCtxPool(conf)

    # get objects of all namespaces for every pool
    ceph_pools = {'commits': args.commits,
                  'fs': args.fs, 'blocks': args.blocks}

    for objtype, poolname in ceph_pools.items():
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
    print "uuid,commits_size,commits_obj,fs_size,fs_obj,blocks_size,"\
          "blocks_obj,size,obj"

    for lib in libraries.values():
        print lib


if __name__ == '__main__':
    main()
