"""Tests.
"""
from django.test import TestCase
from api.elastic_queries import ElasticSearchQueries

from elasticmock import elasticmock


elastic_data_mock ={
    'recipe_title': 'omelete',
    'ingredients': 'ovo',
    'page_title': 'melhor omelete',
    'link': 'omeletao.com',
    'images': 'aoOvo.png',
    'raw_text': 'blablabla',
    'group': 'bom demais',
    'comments': 'nem tanto',
    'favorites': 'tem certeza?',
    'preparation_time': '2 minutos é muito',
    'portions': '9 3/4'
}


class SubElasticSearchQueriesForTest(ElasticSearchQueries):
    """
    SubClass that allows to set and read
    mocked data into ElasticSearch database
    """
    def create(self, index, body):
        es_object = self.es.index(index, body)
        return es_object.get('_id')

    def read(self, index, id):
        es_object = self.es.get(index, id)
        return es_object.get('_source')

class ElasticSearchQueriesTestCase(TestCase):

    @elasticmock
    def test_elastic_connection(self):
        index = 'test-index'
        expected_document = {
            'foo': 'bar'
        }
        # Instantiate service
        service = SubElasticSearchQueriesForTest()
        # Index document on ElasticSearch
        id = service.create(index, expected_document)
        self.assertIsNotNone(id)
        # Retrive document from ElasticSearch
        document = service.read(index, id)
        self.assertEquals(expected_document, document)

    @elasticmock
    def test_elastic_search_by_name(self):
        index = 'recipes'
        # Instantiate service
        service = SubElasticSearchQueriesForTest()
        # Create mocked data
        service.create(index, elastic_data_mock)
        # Realize search by name on ElasticSearch
        recipe = service.search_by_name('omelete')
        self.assertIsNotNone(recipe)

    @elasticmock
    def test_elastic_search_by_ingredients(self):
        index = 'recipes'
        # Instantiate service
        service = SubElasticSearchQueriesForTest()
        # Create mocked data
        service.create(index, elastic_data_mock)
        # Realize search by ingredients on ElasticSearch
        ingredient = service.search_by_ingredients('ovo')
        self.assertIsNotNone(ingredient)

    @elasticmock
    def test_elastic_search_by_name_empty(self):
        index = 'recipes'
        # Instantiate service
        service = SubElasticSearchQueriesForTest()
        # Create mocked data
        service.create(index, elastic_data_mock)
        # Realize search by name on ElasticSearch
        recipe = service.search_by_name('')
        self.assertIsNotNone(recipe)

    @elasticmock
    def test_elastic_search_by_ingredients_empty(self):
        index = 'recipes'
        # Instantiate service
        service = SubElasticSearchQueriesForTest()
        # Create mocked data
        service.create(index, elastic_data_mock)
        # Realize search by ingredients on ElasticSearch
        ingredient = service.search_by_ingredients('')
        self.assertIsNotNone(ingredient)

    @elasticmock
    def test_elastic_search_by_name_especial_caracteres(self):
        index = 'recipes'
        # Instantiate service
        service = SubElasticSearchQueriesForTest()
        # Create mocked data
        service.create(index, elastic_data_mock)
        # Realize search by name on ElasticSearch
        recipe = service.search_by_name('@#')
        self.assertIsNotNone(recipe)

    @elasticmock
    def test_elastic_search_by_ingredients_especial_caracteres(self):
        index = 'recipes'
        # Instantiate service
        service = SubElasticSearchQueriesForTest()
        # Create mocked data
        service.create(index, elastic_data_mock)
        # Realize search by ingredients on ElasticSearch
        ingredient = service.search_by_ingredients('@#')
        self.assertIsNotNone(ingredient)


    @elasticmock
    def test_elastic_search_by_name_upper_caracteres(self):
        index = 'recipes'
        # Instantiate service
        service = SubElasticSearchQueriesForTest()
        # Create mocked data
        service.create(index, elastic_data_mock)
        # Realize search by name on ElasticSearch
        recipe = service.search_by_name('AAa')
        self.assertIsNotNone(recipe)

    @elasticmock
    def test_elastic_search_by_ingredients_upper_caracteres(self):
        index = 'recipes'
        # Instantiate service
        service = SubElasticSearchQueriesForTest()
        # Create mocked data
        service.create(index, elastic_data_mock)
        # Realize search by ingredients on ElasticSearch
        ingredient = service.search_by_ingredients('AAa')
        self.assertIsNotNone(ingredient)


    @elasticmock
    def test_elastic_search_by_returning_fields(self):

        index = 'recipes'
        service = SubElasticSearchQueriesForTest()
        service.create(index, elastic_data_mock)
        service.reset_returning_fields([])
        self.assertEquals(service.returning_fields, [])


