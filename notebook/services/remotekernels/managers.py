import os

from tornado import gen, web
from tornado.escape import json_encode, json_decode, url_escape
from tornado.httpclient import AsyncHTTPClient, HTTPError

from ..kernels.kernelmanager import MappingKernelManager
from ..sessions.sessionmanager import SessionManager as BaseSessionManager
from jupyter_client.kernelspec import KernelSpecManager

from traitlets import Dict, Instance

# TODO: do not inherit from MappingKernelManager
class RemoteKernelManager(MappingKernelManager):
    '''
    Kernel manager that supports remote kernels.
    '''

    '''
    url: Base HTTP URL of the remote kernel service
    '''
    # TODO: make configurable
    kg_host = os.getenv('GATEWAY_HOST', 'localhost:9001')
    url = 'http://{}/api/kernels'.format(kg_host)

    _kernels = Dict()
    
    def __contains__(self, kernel_id):
        # TODO: The notebook code base assumes a sync operation to determine if
        # kernel manager has a kernel_id (existing kernel manager stores kernels
        # in dictionary).  Keeping such a dictionary in sync with remote KG is
        # NOT something we want to do, is it?
        #
        # options:
        #  - poll KG on interval to try to keep in sync
        #  - update internal dictionary on every /api/kernels request
        #  - replace `__contains__` with more formal async get_kernel API
        #    (requires notebook code base changes)
        self.log.debug('RemoteKernelManager.__contains__ {}'.format(kernel_id))
        return kernel_id in self._kernels

    def _remove_kernel(self, kernel_id):
        """remove a kernel from our mapping.
        Mainly so that a kernel can be removed if it is already dead,
        without having to call shutdown_kernel.
        The kernel object is returned.
        """
        try:
            return self._kernels.pop(kernel_id)
        except KeyError:
            pass

    def _kernel_id_to_url(self, kernel_id):
        '''
        Builds a url for the given kernel UUID.

        kernel_id: kernel UUID
        '''
        return "{}/{}".format(self.url, str(kernel_id))

    @gen.coroutine
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
        """
        self.log.info(
            'Request start kernel: kernel_id=%s, path="%s"',
            kernel_id, path
        )

        # TODO: handle errors

        client = AsyncHTTPClient()
        if kernel_id is None:
            # if path is not None:
            #     kwargs['cwd'] = self.cwd_for_path(path)
            kernel_name = kwargs.get('kernel_name', 'python3')
            self.log.info("Request new kernel at: %s" % self.url)
            response = yield client.fetch(
                self.url,
                method='POST',
                body=json_encode({'name' : kernel_name})
            )
            kernel = json_decode(response.body)
            kernel_id = kernel['id']
            self.log.info("Kernel started: %s" % kernel_id)
        else:
            kernel = yield self.get_kernel(kernel_id)
            kernel_id = kernel['id']
            self.log.info("Using existing kernel: %s" % kernel_id)
        self._kernels[kernel_id] = kernel
        raise gen.Return(kernel_id)

    @gen.coroutine
    def get_kernel(self, kernel_id=None, **kwargs):
        """Get kernel for kernel_id.

        Parameters
        ----------
        kernel_id : uuid
            The uuid of the kernel.
        """
        client = AsyncHTTPClient()
        kernel_url = self._kernel_id_to_url(kernel_id)
        self.log.info("Request kernel at: %s" % kernel_url)
        try:
            response = yield client.fetch(
                kernel_url,
                method='GET'
            )
        except HTTPError as error:
            if error.code == 404:
                self.log.info("Kernel not found at: %s" % kernel_url)
                self._remove_kernel(kernel_id)
                kernel = None
            # TODO: handle errors
            else:
                raise error
        else:
            kernel = json_decode(response.body)
            self._kernels[kernel_id] = kernel
        self.log.info("Kernel retrieved: %s" % kernel)
        raise gen.Return(kernel)

    @gen.coroutine
    def kernel_model(self, kernel_id):
        """Return a dictionary of kernel information described in the
        JSON standard model."""
        self.log.debug("RemoteKernelManager.kernel_model: %s", kernel_id)
        model = yield self.get_kernel(kernel_id)
        raise gen.Return(model)

    @gen.coroutine
    def list_kernels(self, **kwargs):
        """Get a list of kernels."""
        self.log.info("Request list kernels: %s", kwargs)
        client = AsyncHTTPClient()
        response = yield client.fetch(
            self.url,
            method='GET'
        )
        kernels = json_decode(response.body)
        self._kernels = {x['id']:x for x in kernels}
        raise gen.Return(kernels)

    @gen.coroutine
    def shutdown_kernel(self, kernel_id):
        """Shutdown a kernel by its kernel uuid.

        Parameters
        ==========
        kernel_id : uuid
            The id of the kernel to shutdown.
        """
        self.log.info("Request shutdown kernel: %s", kernel_id)
        client = AsyncHTTPClient()
        kernel_url = self._kernel_id_to_url(kernel_id)
        self.log.info("Request delete kernel at: %s", kernel_url)
        response = yield client.fetch(
            kernel_url,
            method='DELETE'
        )
        self.log.info("Shutdown kernel response: %d %s",
            response.code, response.reason)
        self._remove_kernel(kernel_id)

    @gen.coroutine
    def restart_kernel(self, kernel_id, now=False, **kwargs):
        """Restart a kernel by its kernel uuid.

        Parameters
        ==========
        kernel_id : uuid
            The id of the kernel to restart.
        """
        self.log.info("Request restart kernel: %s", kernel_id)

        client = AsyncHTTPClient()
        kernel_url = self._kernel_id_to_url(kernel_id) + '/restart'
        self.log.info("Request restart kernel at: %s", kernel_url)
        response = yield client.fetch(
            kernel_url,
            method='POST',
            body=json_encode({})
        )
        self.log.info("Restart kernel response: %d %s",
            response.code, response.reason)

    @gen.coroutine
    def interrupt_kernel(self, kernel_id, **kwargs):
        """Interrupt a kernel by its kernel uuid.

        Parameters
        ==========
        kernel_id : uuid
            The id of the kernel to interrupt.
        """
        self.log.info("Request interrupt kernel: %s", kernel_id)

        client = AsyncHTTPClient()
        kernel_url = self._kernel_id_to_url(kernel_id) + '/interrupt'
        self.log.info("Request restart kernel at: %s", kernel_url)
        response = yield client.fetch(
            kernel_url,
            method='POST',
            body=json_encode({})
        )
        self.log.info("Interrupt kernel response: %d %s",
            response.code, response.reason)

    def shutdown_all(self):
        '''
        Shutdown all kernels.
        '''
        # TODO: Do we want to do this for remote kernels?  The only code that
        # invokes this is the notebook app when the server stops, but that
        # assumes the kernels are local subprocesses.
        pass

class RemoteKernelSpecManager(KernelSpecManager):

    '''
    url: Base HTTP URL of the remote kernel service
    '''
    kg_host = os.getenv('GATEWAY_HOST', 'localhost:9001')
    url = 'http://{}/api/kernelspecs'.format(kg_host)

    @gen.coroutine
    def list_kernel_specs(self):
        """Get a list of kernel specs."""
        self.log.info("Request list kernel specs at: %s", self.url)
        client = AsyncHTTPClient()
        response = yield client.fetch(
            self.url,
            method='GET'
        )
        kernel_specs = json_decode(response.body)
        raise gen.Return(kernel_specs)

    @gen.coroutine
    def get_kernel_spec(self, kernel_name, **kwargs):
        """Get kernel spec for kernel_name.

        Parameters
        ----------
        kernel_name : str
            The name of the kernel.
        """
        client = AsyncHTTPClient()
        kernel_spec_url = "{}/{}".format(self.url, str(kernel_name))
        self.log.info("Request kernel spec at: %s" % kernel_spec_url)
        response = yield client.fetch(
            kernel_spec_url,
            method='GET'
        )
        # TODO: if 404, return None
        kernel_spec = json_decode(response.body)
        self.log.info("Kernel spec retrieved: %s" % kernel_spec)
        raise gen.Return(kernel_spec)

class SessionManager(BaseSessionManager):

    kernel_manager = Instance('notebook.services.remotekernels.managers.RemoteKernelManager')

    @gen.coroutine
    def create_session(self, path=None, kernel_name=None, kernel_id=None):
        """Creates a session and returns its model"""
        session_id = self.new_session_id()

        kernel = None
        if kernel_id is not None:
            kernel = yield gen.maybe_future(
                self.kernel_manager.get_kernel(kernel_id)
            )
        
        if kernel is not None:
            pass
        else:
            kernel_id = yield self.start_kernel_for_session(
                session_id, path, kernel_name
            )

        result = yield gen.maybe_future(
            self.save_session(session_id, path=path, kernel_id=kernel_id)
        )
        # py2-compat
        raise gen.Return(result)

    @gen.coroutine
    def save_session(self, session_id, path=None, kernel_id=None):
        """Saves the items for the session with the given session_id
        
        Given a session_id (and any other of the arguments), this method
        creates a row in the sqlite session database that holds the information
        for a session.
        
        Parameters
        ----------
        session_id : str
            uuid for the session; this method must be given a session_id
        path : str
            the path for the given notebook
        kernel_id : str
            a uuid for the kernel associated with this session
        
        Returns
        -------
        model : dict
            a dictionary of the session model
        """
        session = yield super(SessionManager, self).save_session(
            session_id=session_id, path=path, kernel_id=kernel_id
        )
        raise gen.Return(session)

    @gen.coroutine
    def get_session(self, **kwargs):
        """Returns the model for a particular session.
        
        Takes a keyword argument and searches for the value in the session
        database, then returns the rest of the session's info.

        Parameters
        ----------
        **kwargs : keyword argument
            must be given one of the keywords and values from the session database
            (i.e. session_id, path, kernel_id)

        Returns
        -------
        model : dict
            returns a dictionary that includes all the information from the 
            session described by the kwarg.
        """
        session = yield super(SessionManager, self).get_session(**kwargs)
        raise gen.Return(session)

    @gen.coroutine
    def update_session(self, session_id, **kwargs):
        """Updates the values in the session database.
        
        Changes the values of the session with the given session_id
        with the values from the keyword arguments. 
        
        Parameters
        ----------
        session_id : str
            a uuid that identifies a session in the sqlite3 database
        **kwargs : str
            the key must correspond to a column title in session database,
            and the value replaces the current value in the session 
            with session_id.
        """
        session = yield self.get_session(session_id=session_id)

        if not kwargs:
            # no changes
            return

        sets = []
        for column in kwargs.keys():
            if column not in self._columns:
                raise TypeError("No such column: %r" % column)
            sets.append("%s=?" % column)
        query = "UPDATE session SET %s WHERE session_id=?" % (', '.join(sets))
        self.cursor.execute(query, list(kwargs.values()) + [session_id])

    @gen.coroutine
    def row_to_model(self, row):
        """Takes sqlite database session row and turns it into a dictionary"""
        kernel = yield gen.maybe_future(
            self.kernel_manager.get_kernel(row['kernel_id'])
        )
        if kernel is None:
            # The kernel was killed or died without deleting the session.
            # We can't use delete_session here because that tries to find
            # and shut down the kernel.
            self.cursor.execute("DELETE FROM session WHERE session_id=?", 
                                (row['session_id'],))
            raise KeyError

        model = {
            'id': row['session_id'],
            'notebook': {
                'path': row['path']
            },
            'kernel': kernel
        }
        return model

    @gen.coroutine
    def list_sessions(self):
        """Returns a list of dictionaries containing all the information from
        the session database"""
        c = self.cursor.execute("SELECT * FROM session")
        result = []
        # We need to use fetchall() here, because row_to_model can delete rows,
        # which messes up the cursor if we're iterating over rows.
        for row in c.fetchall():
            try:
                model = yield self.row_to_model(row)
                result.append(model)
            except KeyError:
                pass
        return result

    @gen.coroutine
    def delete_session(self, session_id):
        """Deletes the row in the session database with given session_id"""
        session = yield self.get_session(session_id=session_id)
        yield gen.maybe_future(self.kernel_manager.shutdown_kernel(session['kernel']['id']))
        self.cursor.execute("DELETE FROM session WHERE session_id=?", (session_id,))
