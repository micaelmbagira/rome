__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging

def _instance_pcidevs_get_multi(context, instance_uuids, session=None):
    return Query(models.PciDevice, session=session).\
        filter_by(status='allocated').\
        filter(models.PciDevice.instance_uuid.in_(instance_uuids))

# def _instance_system_metadata_get_multi(context, instance_uuids,
#                                         session=None, use_slave=False):
#
#     # for instance_uuid in instance_uuids:
#     #     query = model_query(context, models.InstanceSystemMetadata,
#     #         session=session, use_slave=use_slave).\
#     #         filter(models.InstanceSystemMetadata.instance_uuid==instance_uuid)
#     #     result += query.all()
#
#     return result

def _instance_metadata_get_multi(context, instance_uuids,
                                 session=None, use_slave=False):
    if not instance_uuids:
        return []
    metadata_list = Query(models.InstanceMetadata).all()
    result = filter(lambda m: m.instance_uuid in instance_uuids, metadata_list)
    return result

def _instances_fill_metadata(context, instances,
                             manual_joins=None, use_slave=False):
    """Selectively fill instances with manually-joined metadata. Note that
    instance will be converted to a dict.

    :param context: security context
    :param instances: list of instances to fill
    :param manual_joins: list of tables to manually join (can be any
                         combination of 'metadata' and 'system_metadata' or
                         None to take the default of both)
    """

    def flatten(l):
        return [item for sublist in l for item in sublist]

    uuids = [inst['uuid'] for inst in instances]

    if manual_joins is None:
        manual_joins = ['metadata', 'system_metadata']

    meta = collections.defaultdict(list)
    if 'system_metadata' in manual_joins:
        for instance in instances:
            for metadata in instance.metadata:
                meta[instance.uuid].append(metadata)

    sys_meta = collections.defaultdict(list)
    if 'system_metadata' in manual_joins:
        for instance in instances:
            for system_metadata in instance.system_metadata:
                sys_meta[instance.uuid].append(system_metadata)

    pcidevs = collections.defaultdict(list)
    if 'pci_devices' in manual_joins:
        for row in _instance_pcidevs_get_multi(context, uuids):
            pcidevs[row['instance_uuid']].append(row)

    filled_instances = []
    for inst in instances:
        inst = dict(inst.iteritems())
        # inst['system_metadata'] = sys_meta[inst['uuid']]
        inst['metadata'] = meta[inst['uuid']]
        if 'pci_devices' in manual_joins:
            inst['pci_devices'] = pcidevs[inst['uuid']]
        filled_instances.append(inst)

    return filled_instances

def _network_get_query():
    return Query(models.Network)


from lib.rome.core.orm.query import or_
from lib.rome.core.orm.query import and_

def network_get_all_by_host(host):
    fixed_host_filter = or_(models.FixedIp.host == host,
            and_(models.FixedIp.instance_uuid != None,
                 models.Instance.host == host))
    fixed_ip_query = Query(models.FixedIp.network_id).\
                     outerjoin((models.Instance,
                                models.Instance.uuid ==
                                models.FixedIp.instance_uuid)).\
                     filter(fixed_host_filter)
    # NOTE(vish): return networks that have host set
    #             or that have a fixed ip with host set
    #             or that have an instance with host set
    host_filter = or_(models.Network.host == host,
                      models.Network.id.in_(fixed_ip_query.subquery()))
    return _network_get_query().\
                       filter(host_filter).\
                       all()


def _security_group_get_query():
    query = Query(models.SecurityGroup)
    return query

def _security_group_get_by_names(project_id, group_names):
    """Get security group models for a project by a list of names.
    Raise SecurityGroupNotFoundForProject for a name not found.
    """
    query = _security_group_get_query().\
            filter_by(project_id=project_id).\
            filter(models.SecurityGroup.name.in_(group_names))
    sg_models = query.all()
    if len(sg_models) == len(group_names):
        return sg_models
    # Find the first one missing and raise
    group_names_from_models = [x.name for x in sg_models]
    for group_name in group_names:
        if group_name not in group_names_from_models:
            raise Exception()
    # Not Reached

def _build_instance_get(columns_to_join=None,):
    query = Query(models.Instance)
    if columns_to_join is None:
        columns_to_join = ['metadata', 'system_metadata']
    for column in columns_to_join:
        if column in ['info_cache', 'security_groups']:
            # Already always joined above
            continue
    return query

def _instance_get_by_uuid(uuid, session=None,
                          columns_to_join=None, use_slave=False):
    result = _build_instance_get(columns_to_join=columns_to_join).\
                filter_by(uuid=uuid).\
                first()
    if not result:
        raise Exception()
    return result

def _instance_update(instance_uuid, values, columns_to_join=None):
    instance_ref = _instance_get_by_uuid(instance_uuid, columns_to_join=columns_to_join)
    if "expected_task_state" in values:
        # it is not a db column so always pop out
        expected = values.pop("expected_task_state")
        if not isinstance(expected, (tuple, list, set)):
            expected = (expected,)
        actual_state = instance_ref["task_state"]
        if actual_state not in expected:
            if actual_state == "DELETING":
                raise Exception()
            else:
                raise Exception()
    if "expected_vm_state" in values:
        expected = values.pop("expected_vm_state")
        if not isinstance(expected, (tuple, list, set)):
            expected = (expected,)
        actual_state = instance_ref["vm_state"]
        if actual_state not in expected:
            raise Exception()

def metadata_to_dict(metadata):
    result = {}
    for item in metadata:
        if not item.get('deleted'):
            result[item['key']] = item['value']
    return result

def instance_sys_meta(instance):
    if not instance.get('system_metadata'):
        return {}
    if isinstance(instance['system_metadata'], dict):
        return instance['system_metadata']
    else:
        return metadata_to_dict(instance['system_metadata'])

def _security_group_ensure_default(session=None):
    from lib.rome.core.session.session import Session as Session
    session = Session()

    with session.begin(subtransactions=True):
        try:
            default_group = _security_group_get_by_names(session,
                                                         context.project_id,
                                                         ['default'])[0]
        except exception.NotFound:
            values = {'name': 'default',
                      'description': 'default',
                      'user_id': context.user_id,
                      'project_id': context.project_id}
            default_group = _security_group_create(context, values,
                                                   session=session)
            usage = model_query(context, models.QuotaUsage,
                                read_deleted="no", session=session).\
                     filter_by(project_id=context.project_id).\
                     filter_by(user_id=context.user_id).\
                     filter_by(resource='security_groups')
            # Create quota usage for auto created default security group
            if not usage.first():
                _quota_usage_create(context.project_id,
                                    context.user_id,
                                    'security_groups',
                                    1, 0,
                                    None,
                                    session=session)
            else:
                usage.update({'in_use': int(usage.first().in_use) + 1})
                # TODO (Jonathan): add a "session.add" to ease the session management :)
                session.add(usage)

            default_rules = _security_group_rule_get_default_query(context,
                                session=session).all()
            for default_rule in default_rules:
                # This is suboptimal, it should be programmatic to know
                # the values of the default_rule
                rule_values = {'protocol': default_rule.protocol,
                               'from_port': default_rule.from_port,
                               'to_port': default_rule.to_port,
                               'cidr': default_rule.cidr,
                               'parent_group_id': default_group.id,
                }
                _security_group_rule_create(context,
                                            rule_values,
                                            session=session)
        return default_group

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    result = Query(models.Network).\
                    filter_by(id=2).\
                    first()

    if not result:
        raise Exception()

    # return result

