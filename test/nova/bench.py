__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging
import time

current_milli_time = lambda: int(round(time.time() * 1000))


def compute_ip(network_id, fixed_ip_id):
    digits = [fixed_ip_id / 255, fixed_ip_id % 255]
    return "172.%d.%d.%d" % (network_id, digits[0], digits[1])


def create_mock_data(network_count=3, fixed_ip_count=200):

    for i in range(1, network_count):
        network = models.Network()
        network.id = i
        network.fixed_ips = []
        # network.cidr = IP
        network.save()

    for i in range(1, network_count):
        for j in range(1, fixed_ip_count):
            fixed_ip = models.FixedIp()
            fixed_ip.id = i * fixed_ip_count + j
            fixed_ip.network_id = i
            fixed_ip.address = compute_ip(i, j)
            fixed_ip.save()
    pass



if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)
    # create_mock_data(2, 50)
    #
    # # fixed_ips = Query(models.FixedIp).filter(models.FixedIp.deleted==None).filter(models.FixedIp.deleted==None).filter(models.FixedIp.updated_at!=None).all()
    # # print(fixed_ips)
    #
    # network = Query(models.Network).filter_by(id=1).all()
    # print(network)



    # TEST1

    def _aggregate_get_query(context, model_class, id_field=None, id=None,
                             session=None, read_deleted=None):
        columns_to_join = {models.Aggregate: ['_hosts', '_metadata']}

        query = Query(model_class, session=session,
                            read_deleted=read_deleted)

        # for c in columns_to_join.get(model_class, []):
        #     query = query.options(joinedload(c))

        if id and id_field:
            query = query.filter(id_field == id)

        return query


    aggregate_id = "1"

    print("[aggregate_get] id:%s" % (aggregate_id))
    query = _aggregate_get_query(None,
                                 models.Aggregate,
                                 models.Aggregate.id,
                                 aggregate_id)
    # aggregate = query.first()
    from lib.rome.core.lazy import LazyReference
    aggregate = LazyReference("aggregates", 1, None, None)
    # aggregate.load_relationships()
    print(aggregate)
    print(aggregate.hosts)

    aggregate = Query(models.Aggregate).first()
    print(aggregate)
    print(aggregate.hosts)

    # # TEST2
    #
    # # fixed_ip = models.FixedIp()
    # # fixed_ip.network_id = 1
    # # fixed_ip.address = "172.%d.%d.%d" % (255, 255, 3)
    # # fixed_ip.save()
    # # # fixed_ip.load_relationships()
    # # # fixed_ip.network.load_relationships()
    # # toto = fixed_ip.network.fixed_ips
    # # # toto.__str__()
    # # print(fixed_ip.network.fixed_ips)
    # # print(fixed_ip.network.fixed_ips[0].network.fixed_ips)
    #
    # query = Query(models.Network)
    # network = query.first()
    # print(network.created_at)
    # # network.load_relationships()
    # print(network.fixed_ips)
    # # network.share_address = "toto"
    # # network.save()
    # print("toto")
    #
    # from lib.rome.core.dataformat import get_decoder, get_encoder
    #
    # object_converter = get_encoder()
    # print(object_converter.simplify(network))