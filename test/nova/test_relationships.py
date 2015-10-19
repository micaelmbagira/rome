__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging

def test_relationships_single_str(save_instance=True, save_info_cache=True):
    print("Ensure that foreign keys are working (test 1, save_instance=%s, save_info_cache=%s)" % (save_instance, save_info_cache))

    instance_count = Query(models.Instance).count()

    instance = models.Instance()
    instance.uuid = "uuid_%s" % (instance_count)
    if save_instance:
        instance.save()

    instance_info_cache = models.InstanceInfoCache()
    instance_info_cache.instance_uuid = instance.uuid
    if not save_info_cache:
        instance.save()
    else:
        instance_info_cache.save()

    instance_from_db = Query(models.Instance, models.Instance.id==instance.id).first()
    instance_info_cache_from_db = Query(models.InstanceInfoCache, models.InstanceInfoCache.id==instance_info_cache.id).first()

    assert instance_from_db.id == instance.id
    assert instance_info_cache_from_db.id == instance_info_cache.id

    assert instance_from_db.info_cache is not None
    assert instance_from_db.info_cache.id == instance_info_cache.id

    assert instance_info_cache_from_db.instance is not None
    assert instance_info_cache_from_db.instance.id == instance.id
    assert instance_info_cache_from_db.instance_uuid == instance.uuid


def test_relationships_single_object(save_instance=True, save_info_cache=True):
    print("Ensure that foreign keys are working (test 2, save_instance=%s, save_info_cache=%s)" % (save_instance, save_info_cache))

    instance_count = Query(models.Instance).count()

    instance = models.Instance()
    instance.uuid = "uuid_%s" % (instance_count)
    if save_instance:
        instance.save()

    instance_info_cache = models.InstanceInfoCache()
    instance_info_cache.instance = instance
    if not save_info_cache:
        instance.save()
    else:
        instance_info_cache.save()

    instance_from_db = Query(models.Instance, models.Instance.id==instance.id).first()
    instance_info_cache_from_db = Query(models.InstanceInfoCache, models.InstanceInfoCache.id==instance_info_cache.id).first()

    assert instance_from_db.id == instance.id
    assert instance_info_cache_from_db.id == instance_info_cache.id

    assert instance_from_db.info_cache is not None
    assert instance_from_db.info_cache.id == instance_info_cache.id

    assert instance_info_cache_from_db.instance is not None
    assert instance_info_cache_from_db.instance.id == instance.id
    assert instance_info_cache_from_db.instance_uuid == instance.uuid


def test_relationships_list_int(save_fixed_ip=True):
    print("Ensure that foreign keys are working (test 3, save_fixed_ip=%s)" % (save_fixed_ip))

    network = models.Network()
    network.save()

    fixed_ips = []
    for i in range(0, 5):
        fixed_ip = models.FixedIp()
        fixed_ip.network_id = network.id
        fixed_ips += [fixed_ip]
        if not save_fixed_ip:
            network.save()
        else:
            fixed_ip.save()

    network_from_db = Query(models.Network, models.Network.id==network.id).first()

    for fixed_ip in fixed_ips:

        fixed_ip_from_db = Query(models.FixedIp, models.FixedIp.network_id==network.id, models.FixedIp.id==fixed_ip.id).first()

        assert network_from_db.id == network.id
        assert fixed_ip_from_db.id == fixed_ip.id

        network_from_db.load_relationships()

        assert network_from_db.fixed_ips is not None and len(network_from_db.fixed_ips) > 0
        assert fixed_ip_from_db.id in map(lambda x: x.id, network_from_db.fixed_ips)

        assert fixed_ip_from_db.network is not None
        assert fixed_ip_from_db.network.id == network_from_db.id
        assert fixed_ip_from_db.network_id == network_from_db.id


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    ######################
    # Instance/InfoCache #
    ######################

    test_relationships_single_str()
    test_relationships_single_object()

    # test_relationships_single_str(save_instance=False) # this test is non-sense!
    test_relationships_single_object(save_instance=False)

    # test_relationships_single_str(save_info_cache=False) # this test is non-sense!
    test_relationships_single_object(save_info_cache=False)
    test_relationships_single_object(save_instance=False, save_info_cache=False)

    ######################
    # Network/FixedIp    #
    ######################

    test_relationships_list_int()
    # test_relationships_list_int(save_fixed_ip=False)
