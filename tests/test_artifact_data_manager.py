import json
import unittest
from settings import base
from unittest.mock import patch, mock_open
from generics.artifact import ArtifactDataManager
from generics.file_type_enum import SupportedFileType

class TestLctkArtifactDataManager(unittest.TestCase):

    def setUp(self):
        self.adm = ArtifactDataManager()

    def test_parse_extension(self):
        valid_extension_csv = self.adm._parse_extension('supported_extension.csv')
        self.assertEqual(valid_extension_csv, SupportedFileType.CSV.value)

        valid_extension_json = self.adm._parse_extension('supported_extension.json')
        self.assertEqual(valid_extension_json, SupportedFileType.JSON.value)

        valid_extension_xls = self.adm._parse_extension('supported_extension.xls')
        self.assertEqual(valid_extension_xls, SupportedFileType.XLS.value)

        valid_extension_xlsx = self.adm._parse_extension('supported_extension.xlsx')
        self.assertEqual(valid_extension_xlsx, SupportedFileType.XLSX.value)

        with self.assertRaises(TypeError):
            self.adm._parse_extension('unsupported_extension.txt')

    def test_read_config(self):
        data_dict = {'random': 'json'}
        json_data = json.dumps(data_dict)

        with patch('builtins.open', mock_open(read_data=json_data)) as mock_file:
            config_data = self.adm._read_config('random.json')
            mock_file.assert_called_with(f'{base.CONFIG_PATH}/random.json')
            self.assertEqual(config_data, data_dict)

    def test_read_config_raises_on_non_json(self):
        with self.assertRaises(TypeError):
            self.adm._read_config('random.notjson')
