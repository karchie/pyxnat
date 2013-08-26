### mirror.py
### XNAT archive data mirrors for pyxnat
### Copyright (c) 2013 Washington University School of Medicine
### Author: Kevin A. Archie <karchie@wustl.edu>

import os, shutil, zipfile
import logging

from .uriutil import uri_last

logger = logging.getLogger('pyxnat.archive')

class XNATArchive(object):
    """Base class for XNAT archive data mirrors.
    root_dir : local root path of the archived data
    archive_root : archive path corresponding to mirror root
    """
    def __init__(self, rootdir, archive_root=None):
        self.rootdir = rootdir
        self.archive_root = archive_root
        self.rootlen = len(archive_root) if archive_root else 0

    def get_file(self, path):
        raise NotImplementedError('XNATArchive subclass must override method')

    def get_resource(self, resource, extract=True, dest_dir=None):
        raise NotImplementedError('XNATArchive subclass must override method')

    def local_path(self, archive_path):
        """Convert the provided archive path into a local mirror path"""
        if not self.archive_root:
            return os.path.join(self.rootdir, archive_path)
        elif archive_path.startswith(self.archive_root):
            logger.debug('joining {} to {}', self.rootdir, archive_path)
            return os.path.join(self.rootdir, archive_path[self.rootlen:])
        else:
            raise ValueError('archive path %s does not start with root %s'
                             % archive_path, archive_root)

    def _get_resource_server_root(self, resource):
        file_uris = [r.get('URI')
                     for r in resource.parent().xpath('xnat:file')
                     if r.get('label') == resource.label()]
        return os.path.dirname(file_uris[0])

    def _get_resource_local_root(self, resource):
        """Returns the local path of the archive root."""
        return self.local_path(self._get_resource_server_root(resource))


class LocalXNATArchive(XNATArchive):
    """XNATArchive implementation for the case where the filesystem
    with the XNAT archive is directly accessible. By default, assumes
    the paths are identical, but this may be overriden by setting the
    local root of the XNAT archive (rootdir) and the server root path
    (archive_root).
    """
    def __init__(self, rootdir='/', archive_root=None):
        super(LocalXNATArchive, self).__init__(rootdir, archive_root)
        self.locator = 'absolutePath'

    def get_file(self, fileobj):
        """Ensures that the provided File is locally available.
           Returns the path to the local copy.
        """
        return fileobj.local_path

    def get_resource(self, resource, extract=True, dest_dir=None):
        """Implements resource.get()."""
        if extract:
            if dest_dir:
                return self._copy_resource(resource, dest_dir)
            else:
                return [f.local_path for f in resource.files()]
        else:
            return self._zip_resource(resource, dest_dir or '.')

    def _zip_resource(self, resource, dest_dir):
        """Creates a zip of all files from the given resource in dest_dir."""
        files_cobj = resource.files()
        resource_root = self._get_resource_server_root(resource)
        zip_location = os.path.join(dest_dir, resource._urn + '.zip')
        with zipfile.#ZipFile(zip_location, 'w') as fzip:
            for f in files_cobj:
                fzip.write(f.local_path,
                           os.path.join(resource._urn,
                                        os.path.relpath(f.server_path,
                                                        resource_root)))
        return zip_location

    def _copy_resource(self, resource, dest_dir):
        """Copies all files from the given resource to dest_dir."""
        files_cobj = resource.files()
        resource_root = self._get_resource_server_root(resource)
        paths = []
        for f in files_cobj:
            path = os.path.join(dest_dir,
                                os.path.relpath(f.server_path, resource_root))
            shutil.copyfile(f.local_path, path)
            paths.append(path)
        return paths

class MirrorXNATArchive(XNATArchive):
    """XNATArchive implementation that downloads files on request,
    incrementally building a mirror of the archived data.
    """
    def __init__(self, rootdir, archive_root):
        super(MirrorXNATArchive, self).__init__(rootdir, archive_root)
        self.locator = 'absolutePath'

    def get_file(self, fileobj):
        """Gets the local path for the named file from the archive."""
        if not os.path.isfile(fileobj.local_path):
            fileobj._get(fileobj.local_path)
        return local_path

    def _fill_resource_holes(self, resource):
        paths = []
        for fileobj in resource.files():
            # fill gaps in the local mirror
            if not os.path.isfile(fileobj.local_path):
                logger.debug('{} is missing from {} mirror; downloading to {}',
                             fileobj._urn, resource._urn, fileobj.local_path)
                fileobj._get(fileobj.local_path)
            paths.append(fileobj.local_path)
        return paths
        
    def _get_resource_files(self, resource):
        """Ensures the existence of a full local copy of the resource.
           Returns the pathnames of all contained files.
        """
        resource_root = self._get_resource_local_root(resource)
        if os.path.isfile(resource_root):
            return self._fill_resource_holes(resource)
        else:
            return resource._get(resource_root, extract=True)

    def get_resource(self, resource, extract=True, dest_dir=None):
        """Implements resource.get()."""
        if extract:
            if dest_dir:
                return self._copy_resource(resource, dest_dir)
            else:
                return self._get_resource_files(resource)
        else:
            return self._zip_resource(resource, dest_dir or '.')

    def _copy_resource(self, resource, dest_dir):
        """Copies the resource contents to dest_dir, downloading
        contents as necessary."""
        resource_root = self._get_resource_local_root(resource)
        shutil.copytree(resource_root, dest_dir)
        return [os.path.join(dest_dir, os.path.relpath(f, resource_root))
                for f in self._get_resource_files(resource)]

    def _zip_resource(self, resource, dest_dir):
        """Builds a zip archive of the named resource, downloading
        contents as necessary.
        """
        resource_root = self._get_resource_local_root(resource)
        logger.debug('resource home {}', resource_root)
        try:
            os.makedirs(dest_dir)
        except OSError:
            pass
        if os.path.isfile(resource_root):
            paths = self._fill_resource_holes(resource)
            zippath = os.path.join(dest_dir, uri_last(resource._uri)+'.zip')
            logger.debug('zipping resource {} to {}', resource_root, zippath)
            with zipfile.ZipFile(zippath, 'w') as fzip:
                for path in paths:
                    fzip.write(path, os.path.relpath(path, resource_root))
            return zippath
        else:
            zippath = resource._get(dest_dir, extract=False)
            # Copy the contents into the mirror 
            with zipfile.ZipFile(zippath, 'r') as fzip:
                fzip.extractall(path=os.path.dirname(resource_root))
            return zippath
