### mirror.py
### XNAT archive data mirrors for pyxnat
### Copyright (c) 2013 Washington University School of Medicine
### Author: Kevin A. Archie <karchie@wustl.edu>

import os, shutil, zipfile

from .uriutil import uri_last

class XNATArchive(object):
    """Base class for XNAT archive data mirrors.

    root_dir : local root path of the archived data

    archive_root : archive path corresponding to mirror root

    Concrete classes must define:
      get_file(self, path)

      _copy_resource(self, resource, dest_dir, extract=True)

      _get_resource_local_root(self, resource)
    """

    def __init__(self, rootdir, archive_root=None):
        self.rootdir = rootdir
        self.archive_root = archive_root
        self.rootlen = len(archive_root) if archive_root else 0

    def local_path(self, archive_path):
        """Convert the provided archive path into a local mirror path"""
        if not self.archive_root:
            return os.path.join(self.rootdir, archive_path)
        elif archive_path.startswith(self.archive_root):
            return os.path.join(self.rootdir, archive_path.sub[self.rootlen:])
        else:
            raise ValueError('archive path %s does not start with root %s'
                             % archive_path, archive_root)

    def _get_resource_server_root(self, resource):
        file_uris = [r.get('URI')
                     for r in resource.parent().xpath('xnat:file')
                     if r.get('label') == resource.label()]
        return os.path.dirname(file_uris[0])


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

    def get_file(self, path):
        """Gets the local path for the named file from the archive."""
        return self.local_path(path)

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
        fzip = zipfile.ZipFile(zip_location, 'w')
        for f in files_cobj:
            fzip.write(f.local_path,
                       os.path.join(resource._urn,
                                    os.path.relpath(f.server_path,
                                                    resource_root)))
        fzip.close()
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

    def _get_resource_local_root(self, resource):
        """Returns the local path of the archive root."""
        return self.local_path(self._get_resource_server_root(resource))
