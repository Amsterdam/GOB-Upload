import datetime

from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobupload.relate.relate import relate, _handle_relations, _remove_gaps
from gobupload.storage.relate import get_relations, _get_data, _convert_row


@patch('gobupload.relate.relate.logger', MagicMock())
class TestRelate(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_empty(self):
        result = _handle_relations([])
        self.assertEqual(result, [])

    @patch('gobupload.relate.relate._handle_relations')
    @patch('gobupload.relate.relate.get_relations')
    def test_relate(self, mock_get_relations, mock_handle_relations):
        mock_get_relations.return_value = [], True, True
        result = relate("catalog", "collection", "field")
        self.assertEqual(result, ([], True, True))
        mock_handle_relations.assert_not_called()

    @patch('gobupload.relate.relate._handle_relations')
    @patch('gobupload.relate.relate.get_relations')
    def test_relate(self, mock_get_relations, mock_handle_relations):
        mock_get_relations.return_value = [1], True, True
        mock_handle_relations.return_value = []
        relate("catalog", "collection", "field")
        mock_handle_relations.assert_called_with([1])


@patch('gobupload.relate.relate.logger', MagicMock())
class TestRelateNoStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'src_dst_1',
                'dst__id': 'dst_1'
            }
        ]
        expect = [
            {
                "src": {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{
                            'source': 'src_dst_1',
                            'id': 'dst_1',
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_single_more(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'src_dst_1',
                'dst__id': 'dst_1'
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_2',
                'dst__source': 'src_dst_1',
                'dst__id': 'dst_2'
            }
        ]
        expect = [
            {
                "src": {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{
                            'source': 'src_dst_1',
                            'id': 'dst_1',
                            'volgnummer': None
                        }]
            },
            {
                "src": {
                    'source': 'src_src_1',
                    'id': 'src_2',
                    'volgnummer': None
                },
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{
                            'source': 'src_dst_1',
                            'id': 'dst_2',
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'src_dst_1',
                'dst__id': 'dst_1'
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'src_dst_1',
                'dst__id': 'dst_2'
            }
        ]
        expect = [
            {
                "src": {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{
                            'source': 'src_dst_1',
                            'id': 'dst_1',
                            'volgnummer': None
                        }, {
                            'source': 'src_dst_1',
                            'id': 'dst_2',
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many_more(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'src_dst_1',
                'dst__id': 'dst_1'
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'src_dst_1',
                'dst__id': 'dst_2'
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_2',
                'dst__source': 'src_dst_1',
                'dst__id': 'dst_3'
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_2',
                'dst__source': 'src_dst_1',
                'dst__id': 'dst_4'
            }
        ]
        expect = [
            {
                "src": {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{
                            'source': 'src_dst_1',
                            'id': 'dst_1',
                            'volgnummer': None
                        }, {
                            'source': 'src_dst_1',
                            'id': 'dst_2',
                            'volgnummer': None
                        }]
            },
            {
                "src": {
                    'source': 'src_src_1',
                    'id': 'src_2',
                    'volgnummer': None
                },
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{
                            'source': 'src_dst_1',
                            'id': 'dst_3',
                            'volgnummer': None
                        }, {
                            'source': 'src_dst_1',
                            'id': 'dst_4',
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)


@patch('gobupload.relate.relate.logger', MagicMock())
class TestRelateBothStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_empty_start(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2007, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_multi_multi(self):
        relations = [
            {
                'src__date_deleted': None,
                'src__source': 'source',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2000, 1, 1),
                'src_eind_geldigheid': None,
                'dst__date_deleted': None,
                'dst__source': 'source',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2001, 1, 1),
                'dst_eind_geldigheid': datetime.date(2010, 1, 1),
                'dst_match_code': 'match_1'
            },
            {
                'src__date_deleted': None,
                'src__source': 'source',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2000, 1, 1),
                'src_eind_geldigheid': None,
                'dst__date_deleted': None,
                'dst__source': 'source',
                'dst__id': 'dst_2',
                'dst_volgnummer': '2',
                'dst_begin_geldigheid': datetime.date(2001, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst_match_code': 'match_2'
            },
            {
                'src__date_deleted': None,
                'src__source': 'source',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2000, 1, 1),
                'src_eind_geldigheid': None,
                'dst__date_deleted': None,
                'dst__source': 'source',
                'dst__id': 'dst_3',
                'dst_volgnummer': '3',
                'dst_begin_geldigheid': datetime.date(2002, 1, 1),
                'dst_eind_geldigheid': datetime.date(2008, 1, 1),
                'dst_match_code': 'match_3'
            }
        ]
        expect = [{
                      'src': {
                          'source': 'source',
                          'id': 'src_1',
                          'volgnummer': '1'
                      },
                      'begin_geldigheid': datetime.date(2000, 1, 1),
                      'eind_geldigheid': datetime.date(2001, 1, 1),
                      'dst': [{
                                  'source': 'source',
                                  'id': None,
                                  'volgnummer': None
                              }]
                  }, {
                      'src': {
                          'source': 'source',
                          'id': 'src_1',
                          'volgnummer': '1',
                          'bronwaardes': ['match_1', 'match_2']
                      },
                      'begin_geldigheid': datetime.date(2001, 1, 1),
                      'eind_geldigheid': datetime.date(2002, 1, 1),
                      'dst': [{
                                  'source': 'source',
                                  'id': 'dst_1',
                                  'volgnummer': '1',
                                  'bronwaardes': ['match_1']
                              }, {
                                  'source': 'source',
                                  'id': 'dst_2',
                                  'volgnummer': '2',
                                  'bronwaardes': ['match_2']
                              }]
                  }, {
                      'src': {
                          'source': 'source',
                          'id': 'src_1',
                          'volgnummer': '1',
                          'bronwaardes': ['match_1', 'match_2', 'match_3']
                      },
                      'begin_geldigheid': datetime.date(2002, 1, 1),
                      'eind_geldigheid': datetime.date(2008, 1, 1),
                      'dst': [{
                                  'source': 'source',
                                  'id': 'dst_1',
                                  'volgnummer': '1',
                                  'bronwaardes': ['match_1']
                              }, {
                                  'source': 'source',
                                  'id': 'dst_2',
                                  'volgnummer': '2',
                                  'bronwaardes': ['match_2']
                              }, {
                                  'source': 'source',
                                  'id': 'dst_3',
                                  'volgnummer': '3',
                                  'bronwaardes': ['match_3']
                              }]
                  }, {
                      'src': {
                          'source': 'source',
                          'id': 'src_1',
                          'volgnummer': '1',
                          'bronwaardes': ['match_1', 'match_2']
                      },
                      'begin_geldigheid': datetime.date(2008, 1, 1),
                      'eind_geldigheid': datetime.date(2010, 1, 1),
                      'dst': [{
                                  'source': 'source',
                                  'id': 'dst_1',
                                  'volgnummer': '1',
                                  'bronwaardes': ['match_1']
                              }, {
                                  'source': 'source',
                                  'id': 'dst_2',
                                  'volgnummer': '2',
                                  'bronwaardes': ['match_2']
                              }]
                  }, {
                      'src': {
                          'source': 'source',
                          'id': 'src_1',
                          'volgnummer': '1',
                          'bronwaardes': ['match_2']
                      },
                      'begin_geldigheid': datetime.date(2010, 1, 1),
                      'eind_geldigheid': datetime.date(2011, 1, 1),
                      'dst': [{
                                  'source': 'source',
                                  'id': 'dst_2',
                                  'volgnummer': '2',
                                  'bronwaardes': ['match_2']
                              }]
                  }, {
                      'src': {
                          'source': 'source',
                          'id': 'src_1',
                          'volgnummer': '1'
                      },
                      'begin_geldigheid': datetime.date(2011, 1, 1),
                      'eind_geldigheid': None,
                      'dst': [{
                                  'source': 'source',
                                  'id': None,
                                  'volgnummer': None
                              }]
                  }]

        result = _handle_relations(relations)
        print("Result", result)
        self.assertEqual(result, expect)

    def test_multi_periods(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2008, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '2',
                'dst_begin_geldigheid': datetime.date(2008, 1, 1),
                'dst_eind_geldigheid': datetime.date(2009, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '3',
                'dst_begin_geldigheid': datetime.date(2009, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [{
            'src': {
                'source': 'src_src_1',
                'id': 'src_1',
                'volgnummer': '1'
            },
            'begin_geldigheid': datetime.date(2006, 1, 1),
            'eind_geldigheid': datetime.date(2007, 1, 1),
            'dst': [{
                'source': 'dst_src_1',
                'id': None,
                'volgnummer': None
            }]
        }, {
            'src': {
                'source': 'src_src_1',
                'id': 'src_1',
                'volgnummer': '1'
            },
            'begin_geldigheid': datetime.date(2007, 1, 1),
            'eind_geldigheid': datetime.date(2008, 1, 1),
            'dst': [{
                'source': 'dst_src_1',
                'id': 'dst_1',
                'volgnummer': '1'
            }]
        }, {
            'src': {
                'source': 'src_src_1',
                'id': 'src_1',
                'volgnummer': '1'
            },
            'begin_geldigheid': datetime.date(2008, 1, 1),
            'eind_geldigheid': datetime.date(2009, 1, 1),
            'dst': [{
                'source': 'dst_src_1',
                'id': 'dst_1',
                'volgnummer': '2'
            }]
        }, {
            'src': {
                'source': 'src_src_1',
                'id': 'src_1',
                'volgnummer': '1'
            },
            'begin_geldigheid': datetime.date(2009, 1, 1),
            'eind_geldigheid': datetime.date(2011, 1, 1),
            'dst': [{
                'source': 'dst_src_1',
                'id': 'dst_1',
                'volgnummer': '3'
            }]
        }]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_empty_end(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2010, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2010, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2010, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_empty_between(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2008, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '2',
                'dst_begin_geldigheid': datetime.date(2009, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2008, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2008, 1, 1),
                'eind_geldigheid': datetime.date(2009, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2009, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '2'
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_empty_begin_end(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2010, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2007, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2010, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2010, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_before(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2000, 1, 1),
                'dst_eind_geldigheid': datetime.date(2006, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_after(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2011, 1, 1),
                'dst_eind_geldigheid': datetime.date(2016, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_before_and_after(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2001, 1, 1),
                'dst_eind_geldigheid': datetime.date(2016, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_more(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2007, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '2',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2007, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '2'
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2012, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2012, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_2',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2012, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2012, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        },
                        {
                            'source': 'dst_src_1',
                            'id': 'dst_2',
                            'volgnummer': '1'
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2011, 1, 1),
                'eind_geldigheid': datetime.date(2012, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_2',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2012, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_2',
                            'volgnummer': '1'
                        }]
            }
        ]

        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many_with_diff(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2008, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2008, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }, {
                            'source': 'dst_src_1',
                            'id': 'dst_2',
                            'volgnummer': '1'
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2008, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_2',
                            'volgnummer': '1'
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)


@patch('gobupload.relate.relate.logger', MagicMock())
class TestRelateNoStatesWithStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                'begin_geldigheid': None,
                'eind_geldigheid': datetime.date(2006, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                'begin_geldigheid': datetime.date(2011, 1, 1),
                'eind_geldigheid': None,
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)


@patch('gobupload.relate.relate.logger', MagicMock())
class TestRelateWithStatesNoStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1'
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2005, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1'
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2'
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2005, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': None
                        },
                        {
                            'source': 'dst_src_1',
                            'id': 'dst_2',
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_multiple_volgnummer(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1'
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '2',
                'src_begin_geldigheid': datetime.date(2007, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2'
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '1'
                },
                'begin_geldigheid': datetime.date(2005, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': None
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': '2'
                },
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_2',
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)


@patch('gobupload.relate.relate.logger', MagicMock())
class TestRelateDateTime(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
                'dst_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0)
            }
        ]
        expect = [
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                'begin_geldigheid': None,
                'eind_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                'begin_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
                'eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
                'dst': [{
                            'source': 'dst_src_1',
                            'id': 'dst_1',
                            'volgnummer': '1'
                        }]
            },
            {
                'src': {
                    'source': 'src_src_1',
                    'id': 'src_1',
                    'volgnummer': None
                },
                'begin_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
                'eind_geldigheid': None,
                'dst': [{
                            'source': 'dst_src_1',
                            'id': None,
                            'volgnummer': None
                        }]
            }
        ]
        result = _handle_relations(relations)
        print("1279", result)
        self.assertEqual(result, expect)

    def test_without_time_with_time(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
            'dst_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0)
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 0, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 0, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
            'dst_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0)
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)

    def test_with_time_without_time(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 12, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.date(2006, 1, 1),
            'dst_eind_geldigheid': datetime.date(2011, 1, 1)
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 12, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.datetime(2006, 1, 1, 0, 0, 0),
            'dst_eind_geldigheid': datetime.datetime(2011, 1, 1, 0, 0, 0)
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)

    def test_with_datetime_no_date(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 12, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': None,
            'dst_eind_geldigheid': None
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 12, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': None,
            'dst_eind_geldigheid': None
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)

    def test_with_date_no_date(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': None,
            'dst_eind_geldigheid': None
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': None,
            'dst_eind_geldigheid': None
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)

    def test_without_time_without_time(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.date(2006, 1, 1),
            'dst_eind_geldigheid': datetime.date(2011, 1, 1)
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.date(2006, 1, 1),
            'dst_eind_geldigheid': datetime.date(2011, 1, 1)
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)


@patch('gobupload.relate.relate.logger', MagicMock())
class TestGaps(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_remove_gaps(self):
        results = []
        expect = []

        result = _remove_gaps(results)
        self.assertEqual(result, expect)

    def test_no_gaps(self):
        results = [{
            "src": {
                "id": 1,
                "volgnummer": None
            },
            "begin_geldigheid": datetime.date(2000, 1, 1),
            "eind_geldigheid": datetime.date(2001, 1, 1)
        }, {
            "src": {
                "id": 1,
                "volgnummer": None
            },
            "begin_geldigheid": datetime.date(2001, 1, 1),
            "eind_geldigheid": datetime.date(2002, 1, 1)
        }
        ]
        expect = results.copy()

        result = _remove_gaps(results)
        self.assertEqual(result, expect)

    def test_gaps(self):
        results = [{
            "src": {
                "id": 1,
                "volgnummer": None
            },
            "begin_geldigheid": datetime.date(2000, 1, 1),
            "eind_geldigheid": datetime.date(2001, 1, 1)
        }, {
            "src": {
                "id": 1,
                "volgnummer": None
            },
            "begin_geldigheid": datetime.date(2002, 1, 1),
            "eind_geldigheid": datetime.date(2003, 1, 1)
        }
        ]
        expect = [results[0]]

        result = _remove_gaps(results)
        self.assertEqual(result, expect)
