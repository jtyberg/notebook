"""A MultiKernelManager for use in the notebook webserver

- raises HTTPErrors
- creates REST API models
"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import os

from tornado import web

from jupyter_client.multikernelmanager import MultiKernelManager
from traitlets import List, Unicode, TraitError

from notebook.utils import to_os_path
from ipython_genutils.py3compat import getcwd


class MappingKernelManager(MultiKernelManager):
    """A KernelManager that handles notebook mapping and HTTP error handling"""

    def _kernel_manager_class_default(self):
        return "jupyter_client.ioloop.IOLoopKernelManager"

    kernel_argv = List(Unicode())

    root_dir = Unicode(config=True)

    def _root_dir_default(self):
        try:
            return self.parent.notebook_dir
        except AttributeError:
            return getcwd()

    def _root_dir_changed(self, name, old, new):
        """Do a bit of validation of the root dir."""
        if not os.path.isabs(new):
            # If we receive a non-absolute path, make it absolute.
            self.root_dir = os.path.abspath(new)
            return
        if not os.path.exists(new) or not os.path.isdir(new):
            raise TraitError("kernel root dir %r is not a directory" % new)

    #-------------------------------------------------------------------------
    # Methods for managing kernels and sessions
    #-------------------------------------------------------------------------

    def _handle_kernel_died(self, kernel_id):
        """notice that a kernel died"""
        self.log.warn("Kernel %s died, removing from map.", kernel_id)
        self.remove_kernel(kernel_id)

    def cwd_for_path(self, path):
        """Turn API path into absolute OS path."""
        os_path = to_os_path(path, self.root_dir)
        # in the case of notebooks and kernels not being on the same filesystem,
        # walk up to root_dir if the paths don't exist
        while not os.path.isdir(os_path) and os_path != self.root_dir:
            os_path = os.path.dirname(os_path)
        return os_path

    def start_kernel(self, kernel_id=None, path=None, **kwargs):
        """Start a kernel for a session and return its kernel_id.

        Parameters
        ----------
        kernel_id : uuid
            The uuid to associate the new kernel with. If this
            is not None, this kernel will be persistent whenever it is
            requested.
        path : API path
            The API path (unicode, '/' delimited) for the cwd.
            Will be transformed to an OS path relative to root_dir.
        kernel_name : str
            The name identifying which kernel spec to launch. This is ignored if
            an existing kernel is returned, but it may be checked in the future.
        """
        if kernel_id is None:
            if path is not None:
                kwargs['cwd'] = self.cwd_for_path(path)
            kernel_id = super(MappingKernelManager, self).start_kernel(
                                            **kwargs)
            self.log.info("Kernel started: %s" % kernel_id)
            self.log.debug("Kernel args: %r" % kwargs)
            # register callback for failed auto-restart
            self.add_restart_callback(kernel_id,
                lambda : self._handle_kernel_died(kernel_id),
                'dead',
            )
        else:
            self._check_kernel_id(kernel_id)
            self.log.info("Using existing kernel: %s" % kernel_id)
        return kernel_id

    def shutdown_kernel(self, kernel_id, now=False):
        """Shutdown a kernel by kernel_id"""
        self._check_kernel_id(kernel_id)
        super(MappingKernelManager, self).shutdown_kernel(kernel_id, now=now)

    def kernel_model(self, kernel_id):
        """Return a dictionary of kernel information described in the
        JSON standard model."""
        self._check_kernel_id(kernel_id)
        model = {"id":kernel_id,
                 "name": self._kernels[kernel_id].kernel_name}
        return model

    def list_kernels(self):
        """Returns a list of kernel_id's of kernels running."""
        kernels = []
        kernel_ids = super(MappingKernelManager, self).list_kernel_ids()
        for kernel_id in kernel_ids:
            model = self.kernel_model(kernel_id)
            kernels.append(model)
        return kernels

    # override _check_kernel_id to raise 404 instead of KeyError
    def _check_kernel_id(self, kernel_id):
        """Check a that a kernel_id exists and raise 404 if not."""
        if kernel_id not in self:
            raise web.HTTPError(404, u'Kernel does not exist: %s' % kernel_id)



## EXPERIMENTAL
import time
import requests
from urllib.parse import urlparse, urljoin
from IPython.utils.traitlets import Unicode
from IPython.utils.path import get_ipython_dir
from IPython.kernel.ioloop import IOLoopKernelManager

class RemoteKernelManager(MappingKernelManager):
    '''
    Kernel manager that supports local kernels as well as remote kernels to be
    provisioned on demand.  Currently supports the RemoteKernelProvisioner for 
    remote kernels only.
    '''
    provisioner = None

    # Location of the ipython directory
    ipython_dir = Unicode()
    def _ipython_dir_default(self):
        return get_ipython_dir()

    def _get_provisioner(self):
        '''
        Gets a kernel provisioner instance.
        '''
        # TODO: can we get the url from application/handler settings?
        return create('http://raam-test.cloudet.xyz:10001')

    def start_kernel(self, kernel_id=None, path=None, kernel_name=None, **kwargs):
        '''
        Starts a kernel for a session and returns its kernel_id. Contacts the
        appropriate provisioner if the kernel has a provisioned kernel spec.

        kernel_id: The uuid to associate the new kernel with. If this
            is not None, this kernel will be persistent whenever it is
            requested.
        path: The API path (unicode, '/' delimited) for the cwd. Will be 
            transformed to an OS path relative to root_dir.
        kernel_name: The name identifying which kernel spec to launch. This is 
            ignored if an existing kernel is returned, but it may be checked in 
            the future.
        '''
        self.log.info('Request start_kernel: kernel_id=%s, kernel_name=%s, path="%s", **kwargs=%s',
            kernel_id, kernel_name, path, kwargs
        )

        provisioner = self._get_provisioner()
        if provisioner is None:
            self.log.info('Provisioning local kernel: %s', kernel_name)
            # local kernel, delegate to base
            kernel_id = super(RemoteKernelManager, self).start_kernel(
                kernel_id, path, kernel_name, **kwargs)
        else:
            self.log.info('Provisioning remote kernel: %s', kernel_name)
            # provisioned kernel, delegate to provisioner
            kernel_info = provisioner.start_kernel(kernel_id, path, kernel_name, 
                **kwargs)
            print('KERNEL_INFO', kernel_info)
            kernel_id = kernel_info['id']
            kernel_name = kernel_info['name']

            # Create a new IOLoopKernelManager using the connection info
            # TODO: should manager always be IOLoopKernelManager?
            model = IOLoopKernelManager(
                parent=self, 
                autorestart=True, 
                kernel_name=kernel_name,
                **kernel_info
            )

            # Store the provisioner instance with the kernel manager so that we
            # can use it later to shutdown, restart, or check kernel status.
            model.provisioner = provisioner

            # Set the session key to the one provided by the provisioner after
            # constructing the kernel manager
            model.session.key = kernel_info.get('key', b'')
            self._kernels[kernel_id] = model

            # We can't invoke start_kernel because it wants to run a kernel via a
            # command line. We touch a few private APIs here.
            # TODO: can we be doing something better?

            # Store the arguments used to launch the kernel. These are 
            # required by the base class for restart_kernel. (TODO: really?)
            model._launch_args = kwargs.copy()
            
            # Intiate the connections.
            model._connect_control_socket()

        return kernel_id

    def restart_kernel(self, kernel_id, now=False, **kwargs):
        '''
        Restart a kernel. Delegate if the kernel was provisioned remotely.
        '''
        self._check_kernel_id(kernel_id)
        model = self._kernels[kernel_id]

        self.log.info("Request restart_kernel: %s, %s", kernel_id, model)
        provisioner = getattr(model, 'provisioner', None)
        if provisioner is None:
            super(RemoteKernelManager, self).restart_kernel(kernel_id, now)
        else:
            print('RESTART', self._kernels)
            # Stop the old
            self.shutdown_kernel(kernel_id, now)
            # Start the new
            self.start_kernel(kernel_name=model.kernel_name, **kwargs)

            # Shuffle the new kernel under the old ID in the dictionary because
            # the handlers assume it will be assigned the same ID. We can't
            # necessarily control that with remote provisioners, so we just
            # map it here. (Ideally, the remote provisioner should accept a 
            # desired ID as input.)
            # self._kernels[kernel_id] = self._kernels[new_kernel_id]
            # self._kernels.pop(new_kernel_id, None)
            print('RESTART', self._kernels)

    def shutdown_kernel(self, kernel_id, now=False):
        '''
        Shutdown a kernel. Delegate if the kernel was provisioned remotely.
        '''
        self._check_kernel_id(kernel_id)
        model = self._kernels[kernel_id]

        self.log.info("Request shutdown_kernel: %s, %s", kernel_id, model)
        provisioner = getattr(model, 'provisioner', None)
        if provisioner is None:
            super(RemoteKernelManager, self).shutdown_kernel(kernel_id, now=now)
        else:
            provisioner.shutdown_kernel(kernel_id)
            self.remove_kernel(kernel_id)

    def shutdown_all(self):
        '''
        Shutdown all kernels.
        '''
        kernel_ids = super(RemoteKernelManager, self).list_kernel_ids()
        for kernel_id in kernel_ids:
            self.shutdown_kernel(kernel_id)

    def is_alive(self, kernel_id):
        '''
        Get if a kernel is alive. Delegate if the kernel was provisioned 
        remotely.
        '''
        self._check_kernel_id(kernel_id)
        model = self._kernels[kernel_id]

        provisioner = getattr(model, 'provisioner', None)
        if provisioner is None:
            return super(RemoteKernelManager, self).is_alive(kernel_id)
        else:
            return provisioner.is_kernel_alive(kernel_id)

class RemoteKernelProvisioner(object):
    '''
    Communicates with a remote kernel gateway provisioning service
    to launch a new kernel, fetch info about an existing kernel, or destroy a 
    kernel.
    '''
    SUCCESS_STATUS   = 200
    CREATED_STATUS   = 201
    NO_RESPONSE      = 204
    RETRY_DELAY_DURATION = 1.0
    REQUESTS_TIMEOUT = 5.0

    def __init__(self, provisioner_url):
        '''
        provisioner_url: Base HTTP URL of the provisioner service
        '''
        self.provisioner_url = provisioner_url + '/api/kernels'

    def _kernel_id_to_url(self, kernel_id):
        '''
        Builds a provisioner for the given kernel UUID.

        kernel_id: UUID of a kernel
        '''
        return "{}/{}".format(self.provisioner_url, str(kernel_id))

    def _post_create_kernel(self):
        '''
        Sends a POST request to the provisioner service. Returns the resource
        URL for the created kernel.

        POST <provisioner_url>
        '''
        response = requests.post(self.provisioner_url, timeout=self.REQUESTS_TIMEOUT)
        if response.status_code == self.CREATED_STATUS:
            kernel_path = response.headers.get("Location", None)
            if kernel_path == None:
                raise RuntimeError('Kernel could not be provisioned!')
            kernel_url = urljoin(self.provisioner_url, kernel_path)
            return kernel_url
        else:
            raise RuntimeError('Kernel could not be provisioned!')

    def _get_kernel_info(self, kernel_url, max_tries=10):
        '''
        Sends a GET request to the provisioner service. Returns the resource
        URL for the created kernel. Retries if the response is NO_RESPONSE to
        account for the case when the kernel is still coming up.

        GET <kernel_url>

        kernel_path: URL of the kernel
        max_tries: Maximum number of times to make the request
        '''
        for i in range(max_tries):
            response = requests.get(kernel_url, timeout=self.REQUESTS_TIMEOUT)
            if response.status_code == self.SUCCESS_STATUS:
                return response.json()
            elif response.status_code != self.NO_RESPONSE:
                raise RuntimeError('Failed to fetch status for {}: {}'.format(
                    kernel_url,
                    response.status_code
                ))
            # wait before trying again
            time.sleep(self.RETRY_DELAY_DURATION)
        else:
            raise RuntimeError('Failed to fetch status for {}: {}'.format(
                    kernel_url,
                    response.status_code
                ))

    def _delete_kernel(self, kernel_url):
        '''
        Sends a DELETE request to the provisioner service.

        DELETE <kernel_url>
        '''
        print('DELETE', kernel_url)
        response = requests.delete(kernel_url, timeout=self.REQUESTS_TIMEOUT)
        if response.status_code == self.NO_RESPONSE:
            return True
        else:
            raise RuntimeError('Failed to delete kernel {}: {}'.format(
                kernel_url,
                response.status_code
            ))

    def get_kernel_info(self, kernel_id):
        '''
        Gets information about a kernel given its UUID. Returns a dictionary 
        containing details for the kernel which can be used to init a 
        ConnectionFileMixin subclass.

        kernel_id: Unique ID of the kernel
        '''
        kernel_url = self._kernel_id_to_url(kernel_id)
        print('INFO', kernel_url)
        # TODO: code originally only tried once, keeping it for now
        return self._get_kernel_info(kernel_url, max_tries=1)

    def is_kernel_alive(self, kernel_id):
        '''
        Gets if the kernel is alive.

        kernel_id: Unique ID of the kernel
        '''
        kernel_url = self._kernel_id_to_url(kernel_id)
        print('ALIVE', kernel_url)
        try:
            # TODO: code originally only tried once, keeping it for now
            self._get_kernel_info(kernel_url, max_tries=1)
        except RuntimeError:
            return False
        return True

    def shutdown_kernel(self, kernel_id):
        '''
        Destroys a kernel. Returns a bool indicating whether the provisioner
        shutdown the kernel successfully or not (e.g., kernel not found).
        '''
        kernel_url = self._kernel_id_to_url(kernel_id)
        print('SHUTDOWN', kernel_url)
        try:
            return self._delete_kernel(kernel_url)
        except RuntimeError:
            return False

    def start_kernel(self, kernel_id, path, kernel_name, **kwargs):
        '''
        Sends a request to create a new kernel. Uses the path from the response 
        to fetch the kernel details. Returns a dictionary containing details
        for the kernel which can be used to init a ConnectionFileMixin 
        subclass.
        '''
        if kernel_id is None:
            kernel_url = self._post_create_kernel()
            kernel_info = self._get_kernel_info(kernel_url)
        else:
            kernel_info = self.get_kernel_info(kernel_id)

        print('START', kernel_info)
        return kernel_info

    def ping(self):
        '''
        Pings the provisioner by fetching the root path.
        '''
        response = requests.get(self.provisioner_url, timeout=self.REQUESTS_TIMEOUT)
        response.raise_for_status()

def create(provisioner_url=None, **kwargs):
    '''
    Creates a provisioner if given a URL to one, else returns None.
    '''
    if provisioner_url is None:
        return None
    else:
        return RemoteKernelProvisioner(provisioner_url)
