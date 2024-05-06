import scrapy
from AISpider.items.dubai_items import DubaiItem
from scrapy.http import JsonRequest
from datetime import datetime, timedelta
from common._date import get_all_month_
import requests

class DubaiSpider(scrapy.Spider):
    name = 'dubai'
    allowed_domains = ["dubailand.gov.ae", 'gateway.dubailand.gov.ae']
    start_urls = [
        'https://gateway.dubailand.gov.ae/open-data/transactions',
        'https://gateway.dubailand.gov.ae/open-data/rents',
        'https://gateway.dubailand.gov.ae/open-data/projects',
        'https://gateway.dubailand.gov.ae/open-data/valuations',
        'https://gateway.dubailand.gov.ae/open-data/lands',
        'https://gateway.dubailand.gov.ae/open-data/units',
        'https://gateway.dubailand.gov.ae/open-data/brokers',
        'https://gateway.dubailand.gov.ae/open-data/developers'
    ]
    custom_settings = {
        'LOG_STDOUT': True,
        # 'LOG_FILE': 'scrapy_dubai.log',
    }

    def __init__(self, run_type='all', category=None, days=30, *args, **kwargs):
        super(DubaiSpider, self).__init__(*args, **kwargs)
        self.run_type = run_type
        self.days = int(days)
        self.category = category


    def start_requests(self):
        now = datetime.now()
        date_from = '01/01/2024'
        if self.run_type == 'all':
            all_month = get_all_month_(date_from, now.strftime('%m/%d/%Y'))
        else:
            date_from = (now - timedelta(days=self.days)).date().strftime('%m/%d/%Y')
            all_month = get_all_month_(date_from, now.strftime('%m/%d/%Y'))

        for index, y_date in enumerate(all_month):
            if y_date == all_month[-1]:
                break
            date_from = y_date
            date_to = all_month[index + 1]
            print(f"{date_from}--{date_to}")
            if self.category == 'lands':
                requests_data = [{'url': self.start_urls[4], 'callback': self.parse_land, 'total_pages': 1000, 'base_json_data': {
                    'P_PROJECT': '',
                    'P_MASTER_PROJECT': '',
                    'P_LAND_TYPE_ID': '',
                    'P_AREA_ID': '',
                    'P_IS_FREE_HOLD': '',
                    'P_ZONE_ID': '',
                    'P_PROP_SB_TYPE_ID': '',
                    'P_TAKE': '100',
                    'P_SKIP': '0',
                    'P_SORT': 'LAND_NUMBER_ASC',
                }}]
            elif self.category == 'units':
                requests_data = [{'url': self.start_urls[5], 'callback': self.parse_unit, 'total_pages': 1000, 'base_json_data': {
                    'P_AREA_ID': '',
                    'P_ZONE_ID': '',
                    'P_IS_FREE_HOLD': '1',
                    'P_IS_LEASE_HOLD': '',
                    'P_IS_OFFPLAN': '',
                    'P_TAKE': '100',
                    'P_SKIP': '0',
                    'P_SORT': 'UNIT_NUMBER_ASC',
                }}]
            elif self.category == 'brokers':
                requests_data = [{'url': self.start_urls[6], 'callback': self.parse_broker, 'total_pages': 1000, 'base_json_data': {
                    'P_GENDER': '',
                    'P_TAKE': '100',
                    'P_SKIP': '0',
                    'P_SORT': 'BROKER_NUMBER_ASC',
                }}]
            else:
                requests_data = [
                    {'url': self.start_urls[0], 'callback': self.parse, 'total_pages': 1000, 'base_json_data': {
                        'P_GROUP_ID': '',
                        'P_FROM_DATE': date_from,
                        'P_TO_DATE': date_to,
                        'P_IS_OFFPLAN': '',
                        'P_IS_FREE_HOLD': '',
                        'P_AREA_ID': '',
                        'P_USAGE_ID': '',
                        'P_PROP_TYPE_ID': '',
                        'P_TAKE': '100',
                        'P_SKIP': '0',
                        'P_SORT': 'TRANSACTION_NUMBER_ASC',
                    }},
                    {'url': self.start_urls[1], 'callback': self.parse_rents, 'total_pages': 1000, 'base_json_data': {
                        'P_DATE_TYPE': '0',
                        'P_IS_FREE_HOLD': '',
                        'P_FROM_DATE': date_from,
                        'P_TO_DATE': date_to,
                        'P_VERSION': '',
                        'P_AREA_ID': '',
                        'P_USAGE_ID': '',
                        'P_PROP_TYPE_ID': '',
                        'P_TAKE': '100',
                        'P_SKIP': '0',
                        'P_SORT': 'CONTRACT_NUMBER_ASC',
                    }},
                    {'url': self.start_urls[2], 'callback': self.parse_project, 'total_pages': 1000, 'base_json_data': {
                        'P_DATE_TYPE': '1',
                        'P_FROM_DATE': date_from,
                        'P_TO_DATE': date_to,
                        'P_AREA_ID': '',
                        'P_PRJ_STATUS': '',
                        'P_PRJ_TYPE_ID': '',
                        'P_ZONE_ID': '',
                        'P_TAKE': '100',
                        'P_SKIP': '0',
                        'P_SORT': 'PROJECT_NUMBER_ASC',
                    }},
                    {'url': self.start_urls[3], 'callback': self.parse_valuations, 'total_pages': 1000, 'base_json_data': {
                        'P_AREA_ID': '',
                         'P_FROM_DATE': date_from,
                        'P_TO_DATE': date_to,
                        'P_PROP_TYPE_ID': '',
                        'P_TAKE': '100',
                        'P_SKIP': '0',
                        'P_SORT': 'PROPERTY_TOTAL_VALUE_ASC',
                    }},
                    {'url': self.start_urls[7], 'callback': self.parse_developer, 'total_pages': 1000, 'base_json_data': {
                        'P_NAME': '',
                        'P_TAKE': '100',
                         'P_FROM_DATE': date_from,
                        'P_TO_DATE': date_to,
                        'P_SKIP': '0',
                        'P_SORT': 'DEVELOPER_NUMBER_ASC',
                    }},
                ]

            for data in requests_data:
                yield from self.make_requests(data)

    def make_requests(self, data):
        items_per_page = 100
        max_retries = 10
        for page_num in range(1000):
            json_data = data['base_json_data']
            json_data['P_SKIP'] = str(page_num * items_per_page)

            retries = 0
            while retries < max_retries:
                response = requests.post(data['url'], json=json_data)
                if response.status_code == 200:
                    result = response.json()['response']['result']
                    if result:
                        yield JsonRequest(
                            url=data['url'],
                            data=json_data,
                            callback=data['callback']
                        )
                        break
                    elif not result:
                        break  
                else:
                    retries += 1
            else:
                break  

            if not result:  
                break  
    # def __init__(self):
    #     self.P_FROM_DATE = '01/01/2024',
    #     self.P_TO_DATE = '04/30/2024',
    #
    #
    # def start_requests(self):
    #     requests_data = [
    #         {'url': self.start_urls[0], 'callback': self.parse, 'total_pages': 600, 'base_json_data': {
    #             'P_GROUP_ID': '',
    #             'P_FROM_DATE': self.P_FROM_DATE[0],
    #             'P_TO_DATE': self.P_TO_DATE[0],
    #             'P_IS_OFFPLAN': '',
    #             'P_IS_FREE_HOLD': '',
    #             'P_AREA_ID': '',
    #             'P_USAGE_ID': '',
    #             'P_PROP_TYPE_ID': '',
    #             'P_TAKE': '100',
    #             'P_SKIP': '0',
    #             'P_SORT': 'TRANSACTION_NUMBER_ASC',
    #         }},
    #         {'url': self.start_urls[1], 'callback': self.parse_rents, 'total_pages': 3411, 'base_json_data': {
    #             'P_DATE_TYPE': '0',
    #             'P_IS_FREE_HOLD': '',
    #             'P_FROM_DATE': self.P_FROM_DATE[0],
    #             'P_TO_DATE': self.P_TO_DATE[0],
    #             'P_VERSION': '',
    #             'P_AREA_ID': '',
    #             'P_USAGE_ID': '',
    #             'P_PROP_TYPE_ID': '',
    #             'P_TAKE': '100',
    #             'P_SKIP': '0',
    #             'P_SORT': 'CONTRACT_NUMBER_ASC',
    #         }},
    #         {'url': self.start_urls[2], 'callback': self.parse_project, 'total_pages': 2, 'base_json_data': {
    #             'P_DATE_TYPE': '1',
    #             'P_FROM_DATE': self.P_FROM_DATE[0],
    #             'P_TO_DATE': self.P_TO_DATE[0],
    #             'P_AREA_ID': '',
    #             'P_PRJ_STATUS': '',
    #             'P_PRJ_TYPE_ID': '',
    #             'P_ZONE_ID': '',
    #             'P_TAKE': '100',
    #             'P_SKIP': '0',
    #             'P_SORT': 'PROJECT_NUMBER_ASC',
    #         }},
    #         {'url': self.start_urls[3], 'callback': self.parse_valuations, 'total_pages': 24, 'base_json_data': {
    #             'P_AREA_ID': '',
    #             'P_FROM_DATE': self.P_FROM_DATE[0],
    #             'P_TO_DATE': self.P_TO_DATE[0],
    #             'P_PROP_TYPE_ID': '',
    #             'P_TAKE': '100',
    #             'P_SKIP': '0',
    #             'P_SORT': 'PROPERTY_TOTAL_VALUE_ASC',
    #         }},
    #         {'url': self.start_urls[4], 'callback': self.parse_land, 'total_pages': 1992, 'base_json_data': {
    #             'P_PROJECT': '',
    #             'P_MASTER_PROJECT': '',
    #             'P_LAND_TYPE_ID': '',
    #             'P_AREA_ID': '',
    #             'P_IS_FREE_HOLD': '',
    #             'P_ZONE_ID': '',
    #             'P_PROP_SB_TYPE_ID': '',
    #             'P_TAKE': '100',
    #             'P_SKIP': '0',
    #             'P_SORT': 'LAND_NUMBER_ASC',
    #         }},
    #         {'url': self.start_urls[5], 'callback': self.parse_unit, 'total_pages': 9150, 'base_json_data': {
    #             'P_AREA_ID': '',
    #             'P_ZONE_ID': '',
    #             'P_IS_FREE_HOLD': '1',
    #             'P_IS_LEASE_HOLD': '',
    #             'P_IS_OFFPLAN': '',
    #             'P_TAKE': '100',
    #             'P_SKIP': '0',
    #             'P_SORT': 'UNIT_NUMBER_ASC',
    #         }},
    #         {'url': self.start_urls[6], 'callback': self.parse_broker, 'total_pages': 236, 'base_json_data': {
    #             'P_GENDER': '',
    #             'P_TAKE': '100',
    #             'P_SKIP': '0',
    #             'P_SORT': 'BROKER_NUMBER_ASC',
    #         }},
    #         {'url': self.start_urls[7], 'callback': self.parse_developer, 'total_pages': 2, 'base_json_data': {
    #             'P_NAME': '',
    #             'P_TAKE': '100',
    #             'P_FROM_DATE': self.P_FROM_DATE[0],
    #             'P_TO_DATE': self.P_TO_DATE[0],
    #             'P_SKIP': '0',
    #             'P_SORT': 'DEVELOPER_NUMBER_ASC',
    #         }},
    #     ]
    #     for data in requests_data:
    #         yield from self.make_requests(data)
    #
    # def make_requests(self, data):
    #     total_pages = data['total_pages']
    #     items_per_page = 100
    #     for page_num in range(total_pages+1):
    #         json_data = data['base_json_data']
    #         json_data['P_SKIP'] = str(page_num * items_per_page)
    #         yield JsonRequest(
    #             url=data['url'],
    #             data=json_data,
    #             callback=data['callback']
    #         )

    def parse(self, response, **kwargs):
        data = response.json()['response']['result']
        item = DubaiItem()
        for detail in data:
            item["app_number"] = detail.get("TRANSACTION_NUMBER")
            transaction_date_str = detail.get("INSTANCE_DATE")
            item["transaction_date"] = int(datetime.strptime(transaction_date_str, "%Y-%m-%dT%H:%M:%S").timestamp() if transaction_date_str else 0)
            item["transaction_type"] = detail.get("GROUP_EN")
            item["transaction_sub_type"] = detail.get("PROCEDURE_EN")
            item["registration_type"] = detail.get("IS_OFFPLAN_EN")
            item["free_hold"] = detail.get("IS_FREE_HOLD_EN")
            item["usage_"] = detail.get("USAGE_EN")
            item["area_"] = detail.get("AREA_EN")
            item["property_type"] = detail.get("PROP_TYPE_EN")
            item["property_sub_type"] = detail.get("PROP_SB_TYPE_EN")
            item["amount_"] = detail.get("TRANS_VALUE")
            item["transaction_size"] = detail.get("ACTUAL_AREA")
            item["property_size"] = detail.get("PROCEDURE_AREA")
            item["rooms"] = detail.get("ROOMS_EN")
            item["parking"] = detail.get("PARKING")
            item["nearest_metro"] = detail.get("NEAREST_METRO_EN")
            item["nearest_mall"] = detail.get("NEAREST_MALL_EN")
            item["nearest_landmark"] = detail.get("NEAREST_LANDMARK_EN")
            item["no_of_buyer"] = detail.get("TOTAL_BUYER")
            item["no_of_seller"] = detail.get("TOTAL_SELLER")
            item["master_project"] = detail.get("MASTER_PROJECT_EN")
            item["project_"] = detail.get("PROJECT_EN")
            item['metadata'] = {}
            del item['metadata']
            print(item)
            yield item

    def parse_rents(self, response):
        data = response.json()['response']['result']
        item = DubaiItem()
        for detail in data:
            item['app_number'] = detail.get('CONTRACT_NUMBER')
            registration_date_str = detail.get('REGISTRATION_DATE')
            item['registration_date'] = int(datetime.strptime(registration_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if registration_date_str else 0
            start_date_str = detail.get('START_DATE')
            item['start_date'] = int(datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if start_date_str else 0
            end_date_str = detail.get('END_DATE')
            item['end_date'] = int(datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if end_date_str else 0
            item['transaction_type'] = detail.get('VERSION_EN')
            item['area_'] = detail.get('AREA_EN')
            item['contract_amount'] = detail.get('CONTRACT_AMOUNT')
            item['annual_amount'] = detail.get('ANNUAL_AMOUNT')
            item['free_hold'] = detail.get('IS_FREE_HOLD_EN')
            item['property_size'] = detail.get('ACTUAL_AREA')
            item['property_type'] = detail.get('PROP_TYPE_EN')
            item['property_sub_type'] = detail.get('PROP_SUB_TYPE_EN')
            item['number_rooms'] = detail.get('ROOMS')
            item['usage_'] = detail.get('USAGE_EN')
            item['nearest_metro'] = detail.get('NEAREST_METRO_EN')
            item['nearest_mall'] = detail.get('NEAREST_MALL_EN')
            item['nearest_landmark'] = detail.get('NEAREST_LANDMARK_EN')
            item['parking'] = detail.get('PARKING')
            item['no_of_units'] = detail.get('TOTAL_PROPERTIES')
            item['master_project'] = detail.get('MASTER_PROJECT_EN')
            item['project_'] = detail.get('PROJECT_EN')
            item['metadata'] = {}
            del item['metadata']
            print(item)
            yield item

    def parse_project(self, response):
        data = response.json()['response']['result']
        item = DubaiItem()
        for detail in data:
            item['app_number'] = detail.get('PROJECT_NUMBER')
            item['project_name'] = detail.get('PROJECT_EN')
            item['developer_number'] = detail.get('DEVELOPER_NUMBER')
            item['developer_name'] = detail.get('DEVELOPER_EN')
            start_date_str = detail.get('START_DATE')
            item['start_date'] = int(datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if start_date_str else 0
            end_date_str = detail.get('END_DATE')
            item['end_date'] = int(datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if end_date_str else 0
            adeption_date_str = detail.get('ADOPTION_DATE')
            item['adeption_date'] = int(datetime.strptime(adeption_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if adeption_date_str else 0
            item['project_type'] = detail.get('PRJ_TYPE_EN')
            item['project_value'] = detail.get('PROJECT_VALUE')
            item['escrow_number'] = detail.get('ESCROW_ACCOUNT_NUMBER')
            item['project_status'] = detail.get('PROJECT_STATUS')
            item['completed'] = detail.get('PERCENT_COMPLETED')
            inspection_date_str = detail.get('INSPECTION_DATE')
            item['inspection_date'] = int(datetime.strptime(inspection_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if inspection_date_str else 0
            completion_date_str = detail.get('COMPLETION_DATE')
            item['completed_date'] = int(datetime.strptime(completion_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if completion_date_str else 0
            item['description'] = detail.get('DESCRIPTION_EN')
            item['area_'] = detail.get('AREA_EN')
            item['zone_authority'] = detail.get('ZONE_EN')
            item['total_lands'] = detail.get('CNT_LAND')
            item['total_buildings'] = detail.get('CNT_BUILDING')
            item['total_villas'] = detail.get('CNT_VILLA')
            item['total_units'] = detail.get('CNT_UNIT')
            item['master_project'] = detail.get('MASTER_PROJECT_EN')
            item['metadata'] = {}
            del item['metadata']
            print(item)
            yield item

    def parse_valuations(self, response):
        data = response.json()['response']['result']
        item = DubaiItem()
        for detail in data:
            item['property_total_type'] = detail.get('PROPERTY_TOTAL_VALUE')
            item['area_'] = detail.get('AREA_EN')
            item['property_size'] = detail.get('PROCEDURE_AREA')
            item['procedure_year'] = detail.get('PROCEDURE_YEAR')
            item['app_number'] = detail.get('PROCEDURE_NUMBER')
            transaction_date_str = detail.get('INSTANCE_DATE')
            item['transaction_date'] = int(datetime.strptime(transaction_date_str, "%Y-%m-%dT%H:%M:%S").timestamp() if transaction_date_str else 0)
            item['amount_'] = detail.get('ACTUAL_WORTH')
            item['transaction_size'] = detail.get('PROCEDURE_AREA')
            item['property_type'] = detail.get('PROPERTY_TYPE_EN')
            item['property_sub_type'] = detail.get('PROP_SUB_TYPE_EN')
            item['metadata'] = {}
            del item['metadata']
            print(item)
            yield item

    def parse_land(self, response):
        data = response.json()['response']['result']
        item = DubaiItem()
        for detail in data:
            item['land_number'] = detail.get('LAND_NUMBER')
            item['land_sub_number'] = detail.get('LAND_SUB_NUMBER')
            item['property_sub_type'] = detail.get('PROP_SUB_TYPE_EN')
            item['land_type'] = detail.get('LAND_TYPE_EN')
            item['property_size'] = detail.get('ACTUAL_AREA')
            item['registration_type'] = detail.get('IS_OFFPLAN_EN')
            item['registration_number'] = detail.get('PRE_REGISTRATION_NUMBER')
            item['free_hold'] = detail.get('IS_FREE_HOLD_EN')
            item['zip_code'] = detail.get('DM_ZIP_CODE')
            item['municipality_number'] = detail.get('MUNICIPALITY_NUMBER')
            item['separated_form'] = detail.get('SEPARATED_FROM')
            item['separated_reference'] = detail.get('SEPARATED_REFERENCE')
            item['master_project'] = detail.get('MASTER_PROJECT_EN')
            item['project_number'] = detail.get('PROJECT_NUMBER')
            item['project_name'] = detail.get('PROJECT_EN')
            item['area_'] = detail.get('AREA_EN')
            item['zone_'] = detail.get('ZONE_EN')
            item['metadata'] = {}
            del item['metadata']
            print(item)
            yield item

    def parse_unit(self, response):
        data = response.json()['response']['result']
        item = DubaiItem()
        for detail in data:
            item['unit_number'] = detail.get('UNIT_NUMBER')
            item['property_sub_type'] = detail.get('PROP_SUB_TYPE_EN')
            item['property_size'] = detail.get('ACTUAL_AREA')
            item['balcony_area'] = detail.get('BALCONY_AREA')
            item['common_area'] = detail.get('COMMON_AREA')
            item['actual_common_area'] = detail.get('ACTUAL_COMMON_AREA')
            item['floor_'] = detail.get('FLOOR')
            item['room_type'] = detail.get('ROOM_EN')
            item['parking'] = detail.get('PARKING_NUMBER')
            item['parking_allocation_type'] = detail.get('PAT_EN')
            creation_date_str = detail.get('CREATION_DATE')
            item['creation_date'] = int(datetime.strptime(creation_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if creation_date_str else 0
            item['registration_type'] = detail.get('IS_OFFPLAN_EN')
            item['registration_number'] = detail.get('PRE_REGISTRATION_NUMBER')
            item['free_hold'] = detail.get('IS_FREE_HOLD_EN')
            item['lease_hold'] = detail.get('IS_LEASE_HOLD_EN')
            item['building_number'] = detail.get('BUILDING_NUMBER')
            item['app_number'] = detail.get('BUILDING_EN')
            item['land_number'] = detail.get('LAND_NUMBER')
            item['land_sub_number'] = detail.get('LAND_SUB_NUMBER')
            item['land_type'] = detail.get('LAND_TYPE_EN')
            item['zip_code'] = detail.get('DM_ZIP_CODE')
            item['municipality_number'] = detail.get('MUNICIPALITY_NUMBER')
            item['master_project'] = detail.get('MASTER_PROJECT_EN')
            item['project_number'] = detail.get('PROJECT_NUMBER')
            item['area_'] = detail.get('AREA_EN')
            item['zone_'] = detail.get('ZONE_EN')
            item['metadata'] = {}
            del item['metadata']
            print(item)
            yield item

    def parse_broker(self, response):
        data = response.json()['response']['result']
        item = DubaiItem()
        for detail in data:
            item['broker_number'] = detail.get('BROKER_NUMBER')
            item['broker_name'] = detail.get('BROKER_EN')
            item['gender_'] = detail.get('GENDER_EN')
            start_date_str = detail.get('LICENSE_START_DATE')
            item['start_date'] = int(datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if start_date_str else 0
            end_date_str = detail.get('LICENSE_END_DATE')
            item['end_date'] = int(datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if end_date_str else 0
            item['website'] = detail.get('WEBPAGE')
            item['phone'] = detail.get('PHONE')
            item['fax_'] = detail.get('FAX')
            item['real_estate_number'] = detail.get('REAL_ESTATE_NUMBER')
            item['real_estate_name'] = detail.get('REAL_ESTATE_EN')
            item['metadata'] = {}
            del item['metadata']
            print(item)
            yield item

    def parse_developer(self, response):
        data = response.json()['response']['result']
        item = DubaiItem()
        for detail in data:
            item['developer_number'] = detail.get('DEVELOPER_NUMBER')
            item['developer_name'] = detail.get('DEVELOPER_EN')
            registration_date_str = detail.get('REGISTRATION_DATE')
            item['registration_date'] = int(datetime.strptime(registration_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if registration_date_str else 0
            item['license_source'] = detail.get('LICENSE_SOURCE_EN')
            item['license_type'] = detail.get('LICENSE_TYPE_EN')
            item['website'] = detail.get('WEBPAGE')
            item['phone'] = detail.get('PHONE')
            item['fax_'] = detail.get('FAX')
            item['license_number'] = detail.get('LICENSE_NUMBER')
            license_issue_date_str = detail.get('LICENSE_ISSUE_DATE')
            item['license_lssue_date'] = int(datetime.strptime(license_issue_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if license_issue_date_str else 0
            license_expiry_date_str = detail.get('LICENSE_EXPIRY_DATE')
            item['license_expiry_date'] = int(datetime.strptime(license_expiry_date_str, "%Y-%m-%dT%H:%M:%S").timestamp()) if license_expiry_date_str else 0
            item['chamber_commerce'] = detail.get('CHAMBER_OF_COMMERCE_NO')
            item['metadata'] = {}
            del item['metadata']
            print(item)
            yield item















