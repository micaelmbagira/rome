__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

from test.nova.methods.test_ensure_default_secgroup import _security_group_ensure_default, _security_group_get_query

import logging
import uuid

LOG = logging.getLogger()

# List of fields that can be joined in DB layer.
_INSTANCE_OPTIONAL_JOINED_FIELDS = ['metadata', 'system_metadata',
                                    'info_cache', 'security_groups',
                                    'pci_devices']
# These are fields that are optional but don't translate to db columns
_INSTANCE_OPTIONAL_NON_COLUMN_FIELDS = ['fault', 'numa_topology',
                                        'pci_requests']

# These are fields that can be specified as expected_attrs
INSTANCE_OPTIONAL_ATTRS = (_INSTANCE_OPTIONAL_JOINED_FIELDS +
                           _INSTANCE_OPTIONAL_NON_COLUMN_FIELDS)

def get_session(use_slave=False, **kwargs):
    # return FakeSession()
    return RomeSession()
    # return OldRomeSession()


def model_query(context, *args, **kwargs):
    # base_model = kwargs["base_model"]
    # models = args
    return RomeQuery(*args, **kwargs)


def _service_get(context, service_id, with_compute_node=True, session=None,
                 use_slave=False):
    query = model_query(context, models.Service, session=session,
                        use_slave=use_slave).\
                     filter_by(id=service_id)

    # if with_compute_node:
    #     query = query.options(joinedload('compute_node'))

    result = query.first()
    if not result:
        raise Exception()

    return result

def service_update(context, service_id, values):
    session = get_session()
    with session.begin():
        service_ref = _service_get(context, service_id,
                                   with_compute_node=False, session=session)
        if values.keys() == ["report_count"]:
            if not values["report_count"] == service_ref.report_count:
                service_ref.update(values)
                # TODO (Jonathan): add a "session.add" to ease the session management :)
                session.add(service_ref)

    return service_ref


def _report_state(service):
        """Update the state of this service in the datastore."""
        ctxt = Context("project1", "user1")
        state_catalog = {}
        try:
            report_count = service.service_ref['report_count'] + 1
            state_catalog['report_count'] = report_count

            service.service_ref = service_update(ctxt,
                    service.service_ref, state_catalog)

            # TODO(termie): make this pattern be more elegant.
            if getattr(service, 'model_disconnected', False):
                service.model_disconnected = False
                LOG.error('Recovered model server connection!')

        # TODO(vish): this should probably only catch connection errors
        except Exception as e:  # pylint: disable=W0702
            import traceback
            traceback.print_exc()
            if not getattr(service, 'model_disconnected', False):
                service.model_disconnected = True
                LOG.exception('model server went away')

class ModelService(object):
    def __init__(self, service_ref):
        self.service_ref = service_ref
        self.model_disconnected = False

class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id

class ModelInstance(dict):
    def __init__(self):
        self.fields = []
        self.deleted = None
        self.cleaned = None

def test_report_state():
    service_values = {'binary': 'nova-consoleauth', 'compute_node': [], 'deleted': None, 'created_at': None, 'updated_at': None, 'report_count': 0, 'topic': 'consoleauth', 'rome_version_number': 1, 'host': 'orion-3', 'disabled': False, 'deleted_at': None, 'disabled_reason': None, 'id': 5}
    service_ref = models.Service()
    for key in service_values:
        service_ref[key] = service_values[key]
    service_ref.save()

    service = ModelService(service_ref)

    _report_state(service)
    _report_state(service)

if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("project1", "user1")

    test_report_state()
