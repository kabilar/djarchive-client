
import os
import logging

# import ntpath
# import posixpath
import posixpath as ufs

from minio import Minio
from datajoint import config as cfg


log = logging.getLogger(__name__)


class PublicationClient(object):
    def __init__(self, **kwargs):
        '''
        Create a PublicationClient.
        Client code should use the 'client' method.
        '''

        self.bucket = kwargs['bucket']
        self.endpoint = kwargs['endpoint']
        self.access_key = kwargs['access_key']
        self.secret_key = kwargs['secret_key']

        self.client = Minio(self.endpoint, access_key=self.access_key,
                            secret_key=self.secret_key)

    @classmethod
    def client(cls, admin=False):
        '''
        Create a PublicationClient.

        Currently:

            Non-admin usage expects dj.config['custom'] values for:

              - djpub.client.bucket
              - djpub.client.endpoint
              - djpub.client.access_key
              - djpub.client.secret_key

            Admin usage expects dj.config['custom'] values for:

              - djpub.admin.bucket
              - djpub.admin.endpoint
              - djpub.admin.access_key
              - djpub.admin.secret_key

        The configuration mechanism is expected to change to allow for
        more general purpose client usage without requiring extra
        configuration.
        '''

        cfg_key = 'djpub.admin' if admin else 'djpub.client'

        try:

            create_args = {k: cfg['custom']['{}.{}'.format(cfg_key, k)]
                           for k in ('endpoint', 'access_key', 'secret_key',
                                     'bucket')}

        except KeyError:

            msg = 'invalid PublicationClient configuration'
            log.warning(msg)
            raise

        return cls(**create_args)

    def publish(name, revision, source_directory):
        '''
        publish contents of source_directory as the dataset of name/revision
        '''
        raise NotImplementedError('publication not implemented')

    def redact(name, revision):
        '''
        redact (revoke) dataset publication of name/revision
        XXX: safety?
        '''
        raise NotImplementedError('redaction not implemented')

    def datasets(self):
        '''
        return the available datasets as a generator of dataset names
        '''
        for ds in (o for o in self.client.list_objects(self.bucket)
                   if o.is_dir):
            yield ds.object_name.rstrip('/')

    def revisions(self, dataset=None):
        '''
        return the list of available dataset revisions as a generator
        of (dataset_name, dataset_revision) tuples.
        '''
        def _revisions(dataset):
            # s3://bucket/set/revision -> generator(('set', 'revision'), ...)

            pfx = '{}/'.format(dataset)
            for ds in (o for o in self.client.list_objects(
                    self.bucket, prefix=pfx) if o.is_dir):

                yield tuple(ds.object_name.rstrip('/').split(ufs.sep))

        datasets = (dataset,) if dataset else self.datasets()

        for ds in datasets:
            yield from _revisions(ds)

    def retrieve(self, name, revision, target_directory, create_target=False):
        '''
        retrieve a dataset's contents into the top-level of target_directory.

        when create_target is specified, target_directory and parents
        will be created, otherwise, an error is signaled.

        XXX: does top-level make sense, or create dataset|dataset/rev subdir?
        '''
        # XXX: not sure about exist_ok here ...
        os.makedirs(target_directory, exist_ok=True) if create_target else None

        if not os.path.exists(target_directory):
            msg = 'target_directory {} does not exist'.format(target_directory)

            log.warning(msg)

            raise FileNotFoundError(msg)

        pfx = ufs.join(name, revision)

        for obj in self.client.list_objects(
                self.bucket, recursive=True, prefix=pfx):

            assert not obj.is_dir  # 'directories' not in recursive list ...

            # convert source path to source subpath and construct local path.
            # local paths are dealt with using OS path for native support,
            # s3-space paths use posixpath since these are '/' delimited

            spath = obj.object_name  # ds/rev/<...?>/thing

            ssubp = spath.replace(  # <...?>/thing
                ufs.commonprefix((pfx, spath)), '').lstrip('/')

            # target_directory/<...?>/thing
            lpath = os.path.join(target_directory, *ssubp.split(ufs.sep))
            lsubd, _ = os.path.split(lpath)

            # ensure we are not creating outside of target_directory
            assert (os.path.commonprefix((target_directory, lpath))
                    == target_directory)

            log.debug('transferring {} -> {}'.format(spath, lpath))

            os.makedirs(lsubd, exist_ok=True)

            self.client.fget_object(self.bucket, spath, lpath)


# export factory method as function
client = PublicationClient.client
