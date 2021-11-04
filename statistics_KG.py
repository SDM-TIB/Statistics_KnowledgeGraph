import math
import sys
from SPARQLWrapper import SPARQLWrapper, JSON


def get_entity_entropy(sparql, n_triple):
    EE = 0
    n_entity = 0
    offset = 0
    limit = 10000
    while True:
        query = """
            select ?s count(?o) as ?NumOc
                where {
                {?s ?p ?o}
                UNION
                {?o ?p ?s}
                }
                GROUP BY ?s
                OFFSET """ + str(offset) + """
                LIMIT """ + str(limit)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        for r in results['results']['bindings']:
            probability_entity = int(r['NumOc']['value']) / n_triple
            val = -probability_entity * math.log(probability_entity)
            EE += val
        n_entity += len(results['results']['bindings'])
        if len(results['results']['bindings']) < limit:
            return EE, n_entity
        offset += limit


def get_relation_entropy(sparql, n_triple):
    RE = 0
    query = """    
        select ?p count(?o) as ?NumOc
            where {
            {?s ?p ?o}
            }
            GROUP BY ?p
            ORDER BY DESC(?NumOc)
        """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    for r in results['results']['bindings']:
        probability_relation = int(r['NumOc']['value']) / n_triple
        val = -probability_relation * math.log(probability_relation)
        RE += val
    return RE, len(results['results']['bindings'])


def get_triple(sparql):
    query = """    
    select count(?s) as ?count
    where {
        ?s ?predicate ?object
        }
        """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return int(results['results']['bindings'][0]['count']['value'])


def relational_density(n_triple, relation):
    return n_triple / relation


def entity_density(n_triple, entity):
    return 2 * n_triple / entity


def save_statistics(line):
    with open('results.csv', 'w') as file:  # 'a'
        file.write(line)


def main(*args):
    sparql = SPARQLWrapper(args[0])
    n_triple = get_triple(sparql)
    print('triples: ', n_triple)
    EE, entity = get_entity_entropy(sparql, n_triple)
    print('entities: ', entity)
    print('entity_entropy: ', EE)
    RE, relation = get_relation_entropy(sparql, n_triple)
    print('relations: ', relation)
    print('relation_entropy: ', RE)
    RD = relational_density(n_triple, relation)
    print('relational_density: ', RD)
    ED = entity_density(n_triple, entity)
    print('entity_density: ', ED)

    head = 'Triples;|E|;|R|;EE;RE;ED;RD\n'
    line = str(n_triple) + ';' + str(entity) + ';' + str(relation) + ';' + str(EE) + ';' + str(RE) + ';' + str(
        ED) + ';' + str(RD) + '\n'
    save_statistics(head + line)


if __name__ == '__main__':
    main(*sys.argv[1:])
