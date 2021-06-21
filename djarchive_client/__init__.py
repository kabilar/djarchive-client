
import os
import logging

import posixpath as ufs

from itertools import chain
from itertools import repeat

from hashlib import sha256 as sha

from minio import Minio
from datajoint import config as cfg
from tqdm import tqdm


log = logging.getLogger(__name__)


class LoggingContext:
    '''
    Conditional logging context manager via:
    https://docs.python.org/3/howto/logging-cookbook.html
    '''
    def __init__(self, logger, level=None, handler=None, close=True):
        self.logger = logger
        self.level = level
        self.handler = handler
        self.close = close

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)
        if self.handler:
            self.logger.addHandler(self.handler)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
        if self.handler:
            self.logger.removeHandler(self.handler)
        if self.handler and self.close:
            self.handler.close()
        # implicit return of None => don't swallow exceptions


class DJArchiveClient(object):
    '''
    Archive Client class - manages operations to s3/djarchive
    '''

    MANIFEST_FNAME = 'djarchive-manifest.csv'

    def __init__(self, **kwargs):
        '''
        Create a DJArchiveClient.
        Normal client code should use the 'client' method.
        '''
        log.debug('kwargs: {}'.format(dict(kwargs, secret_key='*REDACTED*')))

        self.bucket = kwargs['bucket']
        self.endpoint = kwargs['endpoint']
        self.access_key = kwargs['access_key']
        self.secret_key = kwargs['secret_key']

        self.client = Minio(self.endpoint, access_key=self.access_key,
                            secret_key=self.secret_key)

    @classmethod
    def client(cls, admin=False):
        '''
        Create a DJArchiveClient.

        Currently:

            Admin usage expects dj.config['custom'] values for:

              - djarchive.access_key
              - djarchive.secret_key

            Client and admin usage allow overriding dj.config['custom']
            defaults for:

              - djarchive.bucket
              - djarchive.endpoint

        The configuration mechanism is expected to change to allow for
        more general purpose client usage without requiring extra
        configuration.
        '''
        log.debug('admin: {}'.format(admin))

        dj_custom = cfg.get('custom', {})

        cfg_defaults = {
            'djarchive.bucket': 'djhub.vathes.datapub.elements',
            'djarchive.endpoint': 's3.djhub.io'
        }

        create_args = {k: {**cfg_defaults, **dj_custom}.get(
            'djarchive.{}'.format(k), None)
                       for k in ('endpoint', 'access_key', 'secret_key',
                                 'bucket')}

        if admin and not all(('access_key' in create_args,
                              'secret_key' in create_args)):

            raise AttributeError('admin operation requested w/o credentials.')

        return cls(**create_args)

    def _manifest(self, filepath):
        '''
        Compute the manifest data for the file at filepath.

        Function returns size in bytes and the sha256 hex digest of the file.

        Does not perform path normalization to/from posix path as used within
        the manifest file.
        '''
        fp_sz = os.stat(filepath).st_size

        fp_sha = sha()

        rd_sz = 1024 * 64

        with open(filepath, 'rb') as fh:
            dat = fh.read(rd_sz)
            while dat:
                fp_sha.update(dat)
                dat = fh.read(rd_sz)

        return fp_sz, fp_sha.hexdigest()

    def _normalize_path(self, root_directory, filepath):
        '''
        normlize path from host-local format into storage-side format
        (s3/posixpath)
        '''

        subp = filepath.replace(
            os.path.commonprefix(
                (root_directory, filepath)), '').lstrip(os.path.sep)

        return subp.replace(os.path.sep, ufs.sep)

    def _denormalize_path(self, root_directory, subpath):
        '''
        denormlize path from storage-side format into host-local format
        (os.path)
        '''

        subpath = subpath.replace(ufs.sep, os.path.sep)

        return os.path.join(root_directory, subpath)

    def write_manifest(self, source_directory, overwrite=False):
        '''
        create a manifest for source_directory.

        manifest is of the form:

          size(bytes),hex(sha256),posixpath(subpath)
          ...

        '''
        log.debug('source_directory: {}, overwrite: {}'.format(
            source_directory, overwrite))

        # XXX: some logic duplicated in _upload_creating_manifest -
        #      adjustments here should be audited for impact there as well.

        mani = os.path.join(source_directory, self.MANIFEST_FNAME)

        if os.path.exists(mani) and not overwrite:
            msg = 'djarchive manifest {} already exists and overwrite=False'
            log.warning(msg)
            raise FileExistsError(msg)

        with open(mani, 'wb') as mani_fh:

            for root, dirs, files in os.walk(source_directory):

                for fp in (os.path.join(root, f) for f in files):

                    if fp == mani:
                        continue

                    subp = self._normalize_path(source_directory, fp)

                    print("adding {}".format(subp))

                    fp_sz, fp_sha = self._manifest(fp)

                    ent = '"{}","{}","{}"\n'.format(fp_sz, fp_sha, subp)

                    mani_fh.write(ent.encode())

    def read_manifest(self, source_directory):
        '''
        Read the manifest contents for the dataset within source_directory,
        if available.

        Returns a file-subpath keyed dictionary with each item
        containing a dictionary of the given files size & sha.

        for example:

          {'/etc/passwd': {'size': 512, 'sha': 'deadbeef...'}}

        If no manifest exists, a FileNotFoundError is raised.
        '''
        log.debug('source_directory: {}'.format(source_directory))

        mani = os.path.join(source_directory, self.MANIFEST_FNAME)

        ret = {}

        with open(mani, 'rb') as mani_fh:
            for ent in mani_fh:
                ent = ent.decode().strip().split(',')
                sz, sha, subp = (i.replace('"', '') for i in ent)

                assert subp not in ret  # detect invalid duplicates

                ret[subp] = {'size': int(sz), 'sha': sha}

        return ret

    def upload(self, name, revision, source_directory, display_progress=False):
        '''
        upload contents of source_directory as the dataset of name/revision

        (currently placeholder for API design)
        '''

        log.debug('name: {}, revision: {}, source_directory: {}'.format(
            name, revision, source_directory))

        with LoggingContext(log, level=logging.DEBUG if display_progress
                            else logging.INFO):

            mani_fp = os.path.join(source_directory, self.MANIFEST_FNAME)

            if os.path.exists(mani_fp):
                self._upload_using_manifest(
                    name, revision, source_directory, display_progress)
            else:
                self._upload_creating_manifest(
                    name, revision, source_directory, display_progress)

    def _upload_using_manifest(self, name, revision, source_directory,
                               display_progress):
        '''
        Upload dataset which already has a manifest -
        Expects source directory to match manifest contents;

        Raises FileNotFoundError if files are found in
        source_directory not in the manifest, and ValueError if files
        are found with manifest size/checksum mismatch.
        '''
        log.debug('name: {}, revision: {}, source_directory: {}'.format(
            name, revision, source_directory))

        mani_fp = os.path.join(source_directory, self.MANIFEST_FNAME)

        mani_dat = self.read_manifest(source_directory)

        for root, dirs, files in os.walk(source_directory):

            for fp in (os.path.join(root, f) for f in files):

                if fp == mani_fp:  # defer manifest upload until end
                    continue

                subp = self._normalize_path(source_directory, fp)

                if subp not in mani_dat:
                    msg = 'subpath {} not in manifest'.format(subp)
                    log.error(msg)
                    raise FileNotFoundError(msg)

                fp_sz, fp_sha = self._manifest(fp)

                ref_sz, ref_sha = mani_dat[subp]['size'], mani_dat[subp]['sha']

                if not all((fp_sz == ref_sz, fp_sha == ref_sha)):

                    msg = 'manifest mismatch for {}'.format(subp)
                    msg += ' (sz: {} / ref: {})'.format(fp_sz, ref_sz)
                    msg += ' (sha: {} / ref: {})'.format(fp_sha, ref_sha)

                    log.error(msg)

                    raise ValueError(msg)

                dstp = ufs.join(name, revision, subp)

                self.fput_object(dstp, fp)

        # upload of files complete - send manifest to indicate completeness.
        self.fput_object(ufs.join(name, revision, self.MANIFEST_FNAME),
                         mani_fp, display_progress)

    def _upload_creating_manifest(self, name, revision, source_directory,
                                  display_progress):
        '''
        Upload dataset without manifest -
        Manifest will be generated as part of the upload process.
        '''
        log.debug('name: {}, revision: {}, source_directory: {}'.format(
            name, revision, source_directory))

        # XXX: some logic duplicated from write_manifest -
        #      adjustments here should be audited for impact there as well.

        mani_fp = os.path.join(source_directory, self.MANIFEST_FNAME)

        assert not os.path.exists(mani_fp)

        with open(mani_fp, 'wb') as mani_fh:

            for root, dirs, files in os.walk(source_directory):

                for fp in (os.path.join(root, f) for f in files):

                    if fp == mani_fp:  # defer manifest upload until end
                        continue

                    fp_sz, fp_sha = self._manifest(fp)

                    subp = self._normalize_path(source_directory, fp)

                    dstp = ufs.join(name, revision, subp)

                    self.fput_object(dstp, fp)

                    ent = '"{}","{}","{}"\n'.format(fp_sz, fp_sha, subp)

                    mani_fh.write(ent.encode())

        # upload of files complete - send manifest to indicate completeness.
        self.fput_object(ufs.join(name, revision, self.MANIFEST_FNAME),
                         mani_fp, display_progress)

    def redact(name, revision):
        '''
        redact (revoke) dataset publication of name/revision

        (currently placeholder for API design)

        XXX: workflow data safety concerns?
        '''
        raise NotImplementedError('redaction not implemented')

    def datasets(self):
        '''
        return the available datasets as a generator of dataset names
        '''

        # s3://bucket/dataset -> generator(('dataset'))

        for ds in (o for o in self.client.list_objects(self.bucket)
                   if o.is_dir):
            yield ds.object_name.rstrip('/')

    def revisions(self, dataset=None):
        '''
        return the list of available dataset revisions as a generator
        of (dataset_name, dataset_revision) tuples.
        '''
        def _revisions(dataset):

            # s3://bucket/dataset/revision ->
            #    generator(('dataset', 'revision'), ...)

            pfx = '{}/'.format(dataset)

            for ds in (o for o in self.client.list_objects(
                    self.bucket, prefix=pfx) if o.is_dir):

                yield tuple(ds.object_name.rstrip('/').split(ufs.sep))

        nfound = 0

        datasets = (dataset,) if dataset else self.datasets()

        for ds in datasets:
            for i, r in enumerate(_revisions(ds), start=1):
                nfound = i
                yield r

        if dataset and not nfound:

            msg = 'dataset {} not found'.format(dataset)
            log.debug(msg)
            raise FileNotFoundError(msg)

    def download(self, dataset_name, revision, target_directory,
                 create_target=False, display_progress=False):
        '''
        download a dataset's contents into the top-level of target_directory.

        when create_target is specified, target_directory and parents
        will be created, otherwise, an error is signaled.

        Note: display_progress currently means 'enable debug level logging'
              within function scope - if details are not displayed,
              ensure loggingConfig settings are correct in client code.

              (verified OK for default djarchive CLI script)
        '''

        log.debug(('dataset_name: {}, revision: {}, target_directory: {},'
                   'create_target: {}, display_progress: {}').format(
                       dataset_name, revision, target_directory,
                       create_target, display_progress))

        with LoggingContext(log, level=logging.DEBUG if display_progress
                            else logging.INFO):

            if create_target:  # ensure target directory exists
                os.makedirs(target_directory, exist_ok=True)

            if not os.path.exists(target_directory):
                msg = 'target_directory {} does not exist'.format(
                    target_directory)
                log.warning(msg)
                raise FileNotFoundError(msg)

            pfx = ufs.join(dataset_name, revision)

            # check/fetch dataset manifest
            log.debug('fetching & loading dataset manifest')

            ssubp = ufs.join(pfx, self.MANIFEST_FNAME)
            lpath = os.path.join(target_directory, self.MANIFEST_FNAME)
            lsubd, _ = os.path.split(lpath)

            if not self.client.stat_object(self.bucket, ssubp):
                msg = 'dataset {} revision {} manifest not found'.format(
                    dataset_name, revision)
                log.debug(msg)
                raise FileNotFoundError(msg)

            self.fget_object(ssubp, lpath, display_progress=display_progress)

            mani = self.read_manifest(lsubd)

            # main download loop -
            #
            # iterate over objects,
            # convert full source path to source subpath,
            # construct local path and create local subdirectory in the target
            # then fetch the object into the local path.
            #
            # local paths are dealt with using OS path for native support,
            # paths in the s3 space use posixpath since these are '/' delimited

            nfound, nerr = 0, 0

            obj_iter = self.client.list_objects(
                self.bucket, recursive=True, prefix=pfx)

            for obj in obj_iter:

                assert not obj.is_dir  # dirs not in recursive=True output

                spath = obj.object_name  # ds/rev/<...?>/thing

                ssubp = spath.replace(  # <...?>/thing
                    ufs.commonprefix((pfx, spath)), '').lstrip('/')

                if ssubp == self.MANIFEST_FNAME:
                    continue  # skip manifest re-download

                # target_directory/<...?>/thing
                lpath = os.path.join(target_directory, *ssubp.split(ufs.sep))
                lsubd, _ = os.path.split(lpath)

                # ensure we are not creating outside of target_directory
                assert (os.path.commonprefix((target_directory, lpath))
                        == target_directory)

                # actual download/verification -
                #
                # if the file exists, checksum and skip, falling back to
                #    refetch on checksum/size mismatch.
                #
                # if the file does not exist, download and checksum.

                if os.path.exists(lpath):

                    log.debug('{} exists. verifying integrity.'.format(lpath))
                    lsz, lsha = self._manifest(lpath)

                    if all((lsz == mani[ssubp]['size'],
                            lsha == mani[ssubp]['sha'])):

                        log.debug('integrity check ok. skipping download.')

                        continue  # for obj in obj_iter

                    nerr += 1
                    log.warning('integrity issue. redownloading {}'.format(
                        spath))

                # transfer file
                log.debug('transferring {} to {}'.format(spath, lpath))

                os.makedirs(lsubd, exist_ok=True)

                self.fget_object(spath, lpath, display_progress)

                # check file integrity
                log.debug('verifying integrity of {}'.format(lpath))

                lsz, lsha = self._manifest(lpath)

                if not all((lsz == mani[ssubp]['size'],
                            lsha == mani[ssubp]['sha'])):

                    nerr += 1

                    log.warning('integrity issue fetching {}'.format(spath))

                # and mark as complete
                nfound += 1

            log.info('transfer complete with {} issues.'.format(nerr))

    def fget_object(self, spath, lpath, display_progress=False):
        '''
        Fetch object in spath into local path lpath.

        If display_progress=True, a download progress meter will be displayed.
        '''
        statb = self.client.stat_object(self.bucket, spath)

        chunksz = 1024 ** 2  # 1 MiB (TODO? configurable/tuning?)

        nchunks, leftover = statb.size // chunksz, statb.size % chunksz

        chunker = (chain(repeat(chunksz, nchunks), (leftover,)) if leftover
                   else repeat(chunksz, nchunks))

        chunker = tqdm(chunker, unit='MiB', ncols=60,
                       disable=not display_progress,
                       total=nchunks + 1 if leftover else nchunks)

        offset = 0
        with open(lpath, 'wb') as fh:
            for chunk in chunker:
                dat = self.client.get_object(
                    self.bucket, spath, offset=offset, length=chunk)
                fh.write(dat.data)
                offset += chunk

    def fput_object(self, dpath, lpath, display_progress=False):
        '''
        Upload file to remote path dpath from lpath.
        '''
        # TODO: progressbar
        # (minio api is inconsistent here - allows a 'progress thread'
        #  for whole-file u/l but no per-chunk u/l vs
        #  chunked dl and no- 'progress thread' d/l)

        log.debug('dpath: {}, lpath: {}'.format(lpath, dpath))
        self.client.fput_object(self.bucket, dpath, lpath)


client = DJArchiveClient.client  # export factory method as utility function
