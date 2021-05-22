import scrapelib
import lxml.html
import requests
import typing
import csv


CaseTypes = typing.Sequence[typing.Literal['C', 'R']]
Statuses = typing.Sequence[typing.Literal['Open', 'Closed', 'Open - Blocked']]


class NLRB(scrapelib.Scraper):
    base_url = 'https://www.nlrb.gov'

    def csv_search(self,
                   case_types: typing.Optional[CaseTypes] = None,
                   statuses: typing.Optional[Statuses] = None,
                   date_start: typing.Optional['datetime.date'] = None,
                   date_end: typing.Optional['datetime.date'] = None):

        params = self._prepare_search_params(case_types,
                                             statuses,
                                             date_start,
                                             date_end)

        response = requests.get(self.base_url + '/search/case', params=params)

        page = lxml.html.fromstring(response.text)
        page.make_links_absolute(self.base_url + '/search/case')

        download_link, = page.xpath("//a[@id='download-button']")

        payload = {'cacheId': download_link.get('data-cacheid'),
                   'typeOfReport': download_link.get('data-typeofreport'),
                   'token': ''}

        response = self.post(self.base_url + '/nlrb-downloads/start-download',
                             data=payload)

        result = response.json()['data']
        while not result['finished']:
            response = self.get(self.base_url + '/nlrb-downloads/progress/' + result['id'])
            result = response.json()['data']

        response = self.get(self.base_url + result['filename'], stream=True)
        lines = (line.decode('utf-8') for line in response.iter_lines())
        reader = csv.DictReader(lines)

        for row in reader:
            row['case type'] = self._case_type(row['Case Number'])

            yield row

    def web_search(self,
                   case_types: typing.Optional[CaseTypes] = None,
                   statuses: typing.Optional[Statuses] = None,
                   date_start: typing.Optional['datetime.date'] = None,
                   date_end: typing.Optional['datetime.date'] = None):

        params = self._prepare_search_params(case_types,
                                             statuses,
                                             date_start,
                                             date_end)

        response = requests.get(self.base_url + '/search/case', params=params)

        yield from self._parse_search_results(response)

        for response in self._paginate(response):
            yield from self._parse_search_results(response)




    def _prepare_search_params(self,
                               case_types: typing.Optional[CaseTypes] = None,
                               statuses: typing.Optional[Statuses] = None,
                               date_start: typing.Optional['datetime.date'] = None,
                               date_end: typing.Optional['datetime.date'] = None):

        params = {}
        if case_types:
            params['f[0]'] = '({})'.format(
                ' OR '.join('case_type:' + case_type
                            for case_type in case_types))
        if statuses:
            for i, status in enumerate(statuses):
                params['s[{}]'.format(i)] = status

        if date_start:
            params['date_start'] = date_start.strftime('%m/%d/%Y')
            if not date_end:
                params['date_end'] = datetime.date.today().strftime('%m/%d/%Y')

        if date_end:
            params['date_end'] = date_end.strftime('%m/%d/%Y')
            if not date_start:
                params['date_start'] = '1/1/1960'

        return params

    def _parse_search_results(self, response: 'requests.models.Response'):

        page = lxml.html.fromstring(response.text)
        page.make_links_absolute(self.base_url + '/search/case')

        for result in page.xpath("//div[@class='wrapper-div']"):
            result_dict = {}

            name, = result.xpath("./div[@class='type-div']//a/text()")
            result_dict['name'] = name.strip()

            left_column = result.xpath(".//div[@class='left-div']/strong")
            right_column = result.xpath(".//div[@class='right-div']/strong")
            columns = left_column + right_column

            for header_element in columns:
                header = header_element.text.strip(': ')
                if header == 'Case Number':
                    link = header_element.getnext()
                    case_number = link.text.strip()
                    result_dict[header] = case_number
                    result_dict['url'] = link.get('href')

                    result_dict['case type'] = self._case_type(case_number)

                elif header == 'Date Filed':
                    date_str = header_element.tail
                    result_dict[header] = datetime.datetime.strptime(date_str, '%B %d, %Y').date()
                else:
                    result_dict[header] = header_element.tail

            yield result_dict

    def _case_type(self, case_number: str) -> str:
        if '-RC-' in case_number:
            case_type = 'RC'
        elif '-RM-' in case_number:
            case_type = 'RM'
        elif '-RD-' in case_number:
            case_type = 'RD'
        elif '-UD-' in case_number:
            case_type = 'UD'
        elif '-UC-' in case_number:
            case_type = 'UC'
        elif '-CA-' in case_number:
            case_type = 'CA' # what's this?
        elif '-CD-' in case_number:
            case_type = 'CD'
        elif '-CC-' in case_number:
            case_type = 'CC'
        elif '-CB-' in case_number:
            case_type = 'CB'
        elif '-CB-' in case_number:
            case_type = 'CB'
        elif '-AC-' in case_number:
            case_type = 'AC'
        else:
            print(case_number)
            raise

        return case_type

    def _paginate(self, response: 'requests.models.Response'):

        while True:
            page = lxml.html.fromstring(response.text)
            page.make_links_absolute(self.base_url + '/search/case')

            next_page_links = page.xpath("//a[@rel='next']")
            if not next_page_links:
                break

            next_page_link, = next_page_links

            response = self.get(next_page_link.get('href'))
            print(next_page_link.get('href'))

            yield response

    def case_details(self, case_number: str):
        case_url = self.base_url + '/case/' + case_number
        response = self.get(case_url)

        page = lxml.html.fromstring(response.text)
        page.make_links_absolute(case_url)

        # Case Name
        details = {}
        name, = page.xpath("//h1[@class='uswds-page-title page-title']/text()")
        details['name'] = name.strip()

        # Basic Details
        basic_section, = page.xpath("//div[@class='partition-div']")
        left_column = basic_section.xpath(".//div[@class='left-div']/b")
        right_column = basic_section.xpath(".//div[@class='right-div case-right-div']/b")
        columns = left_column + right_column

        for header_element in columns:
            header = header_element.text.strip(': ')
            if header == 'Case Number':
                case_number = header_element.tail.strip()
                details[header] = case_number
                details['case type'] = self._case_type(case_number)

            elif header == 'Date Filed':
                date_str = header_element.getnext().text.strip()
                details[header] = datetime.datetime.strptime(date_str, '%m/%d/%Y').date()
            else:
                details[header] = header_element.tail.strip()

        # Docket
        docket = []
        if 'Docket Activity data is not available' not in response.text:
            details['docket'] = self._docket(page, case_url)

        related_documents = []
        if 'Related Documents data is not available' not in response.text:
            related_document_header, = page.xpath(".//h2[text()='Related Documents']")
            document_list = related_document_header.getnext().getnext()
            for doc_link in document_list.xpath('.//a'):
                related_documents.append({'name': doc_link.text,
                                          'url': doc_link.get('href')})
        details['related documents'] = related_documents

        if 'Allegations data is not available' not in response.text:
            print(case_number)
            raise

        # Participants
        participants = []
        if 'Participants data is not available' not in response.text:
            participant_table, = page.xpath("//table[starts-with(@class, 'Participant')]/tbody")

            for row in participant_table.xpath('./tr'):
                participant_entry = {}

                participant, address, phone = row.xpath('./td')

                participant_text = [br.tail.strip() for br in participant.xpath('./br') if br.tail]
                participant_entry['type'], *participant_text = participant_text
                participant_entry['participant'] = '\n'.join(participant_text).strip()
                participant_entry['address'] = '\n'.join(line.strip() for line in address.xpath('./text()')).strip()
                participant_entry['phone number'] = phone.text.strip()

                participants.append(participant_entry)

        details['participants'] = participants

        # Related Cases
        details['related cases'] = page.xpath("//table[starts-with(@class, 'related-case')]/tbody//a/text()")

        return details

    def _docket(self, page, case_url):
        # not currently working on https://www.nlrb.gov/case/14-RC-012769?page=1
        docket = []
        
        while True:
            docket_table, = page.xpath("//div[@id='case_docket_activity_data']/table/tbody")
            for row in docket_table.xpath('./tr'):
                docket_entry = {}

                date, document, party = row.xpath('./td')
                docket_entry['date'] = datetime\
                    .datetime\
                    .strptime(date.text.strip(),
                              '%m/%d/%Y').date()
                if document:
                    document_link, = document.xpath('./a')
                    docket_entry['document'] = document_link.text.strip()
                    docket_entry['url'] = document_link.get('href')
                else:
                    docket_entry['document'] = document.text.strip()

                docket_entry['issued by/filed by'] = party.text.strip()

                docket.append(docket_entry)
            
            next_page_links = page.xpath("//a[@rel='next']")
            if not next_page_links:
                break

            next_page_link, = next_page_links

            print(next_page_link.get('href'))
            response = self.get(next_page_link.get('href'))
            
            page = lxml.html.fromstring(response.text)
            page.make_links_absolute(case_url)

        return docket


    def certifications(self,
                       statuses: typing.Optional[Statuses] = None,
                       date_start: typing.Optional['datetime.date'] = None,
                       date_end: typing.Optional['datetime.date'] = None):
        for result in self.csv_search(case_types=['R'],
                                      statuses=statuses,
                                      date_start=date_start,
                                      date_end=date_end):
            if result['case type'] == 'RC':
                result.update(self.case_details(result['Case Number']))
                yield result

    def decertifications(self,
                         statuses: typing.Optional[Statuses] = None,
                         date_start: typing.Optional['datetime.date'] = None,
                         date_end: typing.Optional['datetime.date'] = None):
        for result in self.csv_search(case_types=['R'],
                                      statuses=statuses,
                                      date_start=date_start,
                                      date_end=date_end):
            if result['case type'] in {'RD', 'RM', 'UD'}:
                result.update(self.case_details(result['Case Number']))
                yield result

    def unit_clarifications(self,
                            statuses: typing.Optional[Statuses] = None,
                            date_start: typing.Optional['datetime.date'] = None,
                            date_end: typing.Optional['datetime.date'] = None):
        for result in self.csv_search(case_types=['R'],
                                      statuses=statuses,
                                      date_start=date_start,
                                      date_end=date_end):
            if result['case type'] == 'UC':
                result.update(self.case_details(result['Case Number']))
                yield result

    def tallies(self):
        '''https://www.nlrb.gov/reports/graphs-data/recent-election-results'''

    def recent_filings(self):
        '''https://www.nlrb.gov/reports/graphs-data/recent-filings?f[0]=case_type:R&date_start=05%2F01%2F2021&date_end=05%2F21%2F2021'''
                              


if __name__ == '__main__':
    import datetime
    import pprint

    s = NLRB()
    s.cache_storage = scrapelib.FileCache('cache-directory')
    s.cache_write_only = False
    

    # also 
    pprint.pprint(s.case_details('14-RC-012769'))
    #results = s.certifications(date_start=datetime.date(2021, 5, 1))
    #for i, result in enumerate(results):
    #    pprint.pprint(result)
    #    print(i)
