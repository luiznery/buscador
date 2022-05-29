""" 
Functions that generate Elasticsearch query dictionaries.
"""

group_mapping = {
    'confeitaria': 'Bolos e tortas doces',
    'lanches': 'Lanches',
    'massas': 'Massas',
    'acompanhamentos': 'Saladas, molhos e acompanhamentos',
    'sobremesas': 'Doces e sobremesas',
    'saudavel': 'Alimentação Saudável',
    'carnes': 'Carnes',
    'prato_unico': 'Prato Único',
    'aves': 'Aves',
    'bebidas': 'Bebidas',
    'frutos_do_mar': 'Peixes e frutos do mar',
    'sopas': 'Sopas'
}

def generate_group(params, filters):
    if 'group' in params:
        filters['group'] = [
            group_mapping[group] 
            for group in params['group'].split(',')
        ]

def generate_others(params, filters, p1, p2, field):

    if p1 in params or p2 in params:
        filters[field] = (
            params[p1] if p1 in params else 0,
            params[p2] if p2 in params else 10000000
        )

def querie_range(queries, field_name, field_value):
    if isinstance(field_value, tuple):
        queries.append(
            {
                "range": {
                    field_name: {
                        "gte": field_value[0],
                        "lte": field_value[1]
                    }
                }
            }
        )

def querie_terms(queries, field_name, field_value):
    if isinstance(field_value, list):
        queries.append(
            {
                "terms": {
                    field_name+".keyword": field_value
                }
            }
        )

class FilterUtils:


    def generate_filters(params):
        filters = {}
        generate_group(params, filters)

        generate_others(params, filters, 'time_min', 'time_max', 'preparation_time')
        generate_others(params, filters, 'portions_min', 'portions_max', 'portions')
        generate_others(params, filters, 'favorites_min', 'favorites_max', 'favorites')

        if len(filters) == 0:
            return None
        return filters



    def get_filter_queries(filters):
        """
        Returns a list of query dictionaries for filtering.
            filters: dictionary with the following structure:
                {
                    "<range_fild_name>": (start, end),
                    "<multiple_options_field_name>": [option1, option2, ...],
                    ...
                }
        """
        queries = []
        for field_name, field_value in filters.items():
            querie_range(queries, field_name, field_value)
            querie_terms(queries, field_name, field_value)

        return queries
        

    def get_query_by_name_filtred(name, filters=None, fuzziness=1):
        """
        Returns a query dictionary for searching by name with.
            name: string.
            filters: dictionary with the following structure:
                {
                    "<range_fild_name>": (start, end),
                    "<multiple_options_field_name>": [option1, option2, ...],
                    ...
                }
            fuzziness: int, default 1.
        """
        must = [
            {
                "multi_match": {
                    "query": name,
                    "fields": [
                        "recipe_title^2",
                        "ingredients^2",
                        "raw_text"
                    ],
                    "type": "most_fields",
                    "fuzziness": fuzziness
                }
            }
        ]
        if filters:
            must = must + FilterUtils.get_filter_queries(filters)

        query = {
            "query": {
                "bool": {
                    "must": must
                }
            }
        }
        return query

    def get_query_by_ingredients_filtred(ingredients, filters=None, fuzziness=1):
        """
        Returns a query dictionary for searching by ingredients with.
            ingredients: list of strings.
            filters: dictionary with the following structure:
                {
                    "<range_fild_name>": (start, end),
                    "<multiple_options_field_name>": [option1, option2, ...],
                    ...
                }
            fuzziness: int, default 1.
        """
        must = [
            {
                "match": {
                    "ingredients": {
                        "query": ingredient,
                        "fuzziness": 1
                    },
                }
            } for ingredient in ingredients
        ]
        if filters:
            must = must + FilterUtils.get_filter_queries(filters)

        query = {
            "query": {
                "bool": {
                    "must": must
                }
            }
        }
        return query 

    

