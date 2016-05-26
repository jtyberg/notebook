
.DEFAULT_GOAL=build

build:
	pip install -e . --user

js:
	python setup.py js css

dev: NB_OPTS=--NotebookApp.kernel_manager_class=notebook.services.remotekernels.managers.RemoteKernelManager \
	--NotebookApp.session_manager_class=notebook.services.remotekernels.managers.SessionManager \
	--NotebookApp.kernel_spec_manager_class=notebook.services.remotekernels.managers.RemoteKernelSpecManager
dev:
	jupyter notebook \
		--no-browser \
		--ip=0.0.0.0 \
		--port=8887 \
		--log-level=DEBUG \
		$(NB_OPTS)

.PHONY: build js dev
