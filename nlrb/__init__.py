import datetime
import functools
import os
import sys
import time
import typing
import urllib.parse
import warnings

import lxml.html
import scrapelib
import selenium.webdriver
import selenium.webdriver.chrome.options
import selenium.webdriver.common.by
import selenium.webdriver.support.expected_conditions
import selenium.webdriver.support.ui
import tqdm
from requests.models import PreparedRequest
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver as Chrome

CaseTypes = typing.Sequence[typing.Literal["C", "R"]]
Statuses = typing.Sequence[typing.Literal["Open", "Closed", "Open - Blocked"]]


class NLRB(scrapelib.Scraper):
    base_url = "https://www.nlrb.gov"

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._new_driver()

    def _new_driver(self):
        options = selenium.webdriver.chrome.options.Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")

        service = Service(os.environ["CHROMEDRIVER_PATH"])
        self.driver = Chrome(options=options, service=service)

    def _download_link(
        self,
        search_url: str,
        case_number_field: str,
        case_types: typing.Optional[CaseTypes] = None,
        statuses: typing.Optional[Statuses] = None,
        date_start: typing.Optional["datetime.date"] = None,
        date_end: typing.Optional["datetime.date"] = None,
    ):

        params = self._prepare_search_params(case_types, statuses, date_start, date_end)

        payload = self._click_download_button(search_url, params)

        download_link = (
            self.base_url
            + "/nlrb-downloads/start-download/{type_of_report}/{cache_id}/{download_token}".format(
                **payload
            )
        )

        response = self.get(download_link, verify=False)

        result = response.json()["data"]

        previous = 0
        with tqdm.tqdm(
            total=result["total"], desc="NLRB.gov preparing download"
        ) as pbar:
            while not result["finished"]:
                response = self.get(
                    self.base_url + "/nlrb-downloads/progress/" + str(result["id"]),
                    verify=False,
                )
                result = response.json()["data"]

                # update progress bar
                current = result["processed"]
                pbar.update(current - previous)
                previous = current

        return self.base_url + result["filename"]

    def _click_download_button(self, search_url, params):

        prepared = PreparedRequest()
        prepared.prepare_url(search_url, params)

        self.driver.get(prepared.url)

        wait = selenium.webdriver.support.ui.WebDriverWait(self.driver, 15)
        wait.until(
            selenium.webdriver.support.expected_conditions.presence_of_element_located(
                (selenium.webdriver.common.by.By.ID, "download-button")
            )
        )

        download_link = self.driver.find_element("xpath", "//a[@id='download-button']")
        for retry in range(4):
            try:
                download_token = self.driver.get_cookie("nlrb-dl-sessid")["value"]
            except TypeError:
                time.sleep(1)
                continue
        else:
            download_token = self.driver.get_cookie("nlrb-dl-sessid")["value"]

        payload = dict(
            cache_id=download_link.get_attribute("data-cacheid"),
            type_of_report=download_link.get_attribute("data-typeofreport"),
            download_token=download_token,
        )

        return payload

    def _prepare_search_params(
        self,
        case_types: typing.Optional[CaseTypes] = None,
        statuses: typing.Optional[Statuses] = None,
        date_start: typing.Optional["datetime.date"] = None,
        date_end: typing.Optional["datetime.date"] = None,
    ):

        params = {}
        if case_types:
            params["f[0]"] = "({})".format(
                " OR ".join("case_type:" + case_type for case_type in case_types)
            )
        if statuses:
            for i, status in enumerate(statuses):
                params["s[{}]".format(i)] = status

        if date_start:
            params["date_start"] = date_start.strftime("%m/%d/%Y")
            if not date_end:
                params["date_end"] = datetime.date.today().strftime("%m/%d/%Y")

        if date_end:
            params["date_end"] = date_end.strftime("%m/%d/%Y")
            if not date_start:
                params["date_start"] = "1/1/1960"

        return params

    def _case_type(self, case_number: str) -> str:
        if "-RC-" in case_number:
            case_type = "RC"
        elif "-RM-" in case_number:
            case_type = "RM"
        elif "-RD-" in case_number:
            case_type = "RD"
        elif "-UD-" in case_number:
            case_type = "UD"
        elif "-UC-" in case_number:
            case_type = "UC"
        elif "-CA-" in case_number:
            case_type = "CA"  # what's this?
        elif "-CD-" in case_number:
            case_type = "CD"
        elif "-CC-" in case_number:
            case_type = "CC"
        elif "-CB-" in case_number:
            case_type = "CB"
        elif "-CE-" in case_number:
            case_type = "CE"
        elif "-CP-" in case_number:
            case_type = "CP"
        elif "-CG-" in case_number:
            case_type = "CG"
        elif "-AC-" in case_number:
            case_type = "AC"
        elif "-WH-" in case_number:
            case_type = "WH"
        else:
            print(case_number, file=sys.stderr)
            raise

        return case_type

    filings = functools.partialmethod(
        _download_link,
        search_url=base_url + "/reports/graphs-data/recent-filings",
        case_number_field="Case Number",
    )

    tallies = functools.partialmethod(
        _download_link,
        search_url=base_url + "/reports/graphs-data/recent-election-results",
        case_types=None,
        case_number_field="Case",
    )

    def advanced_search(self, case_number: str):

        search_url = "https://www.nlrb.gov/advanced-search"
        params = {
            "foia_report_type": "cases_and_decisions",
            "cases_and_decisions_cboxes[close_method]": "close_method",
            "cases_and_decisions_cboxes[employees]": "employees",
            "cases_and_decisions_cboxes[union]": "union",
            "cases_and_decisions_cboxes[unit_description]": "unit_description",
            "cases_and_decisions_cboxes[voters]": "voters",
            "cases_and_decisions_cboxes[date_closed]": "date_closed",
            "cases_and_decisions_cboxes[city]": "city",
            "cases_and_decisions_cboxes[state]": "state",
            "cases_and_decisions_cboxes[case]": "case",
            "search_term": f'"{case_number}"',
        }

        prepared = PreparedRequest()
        prepared.prepare_url(search_url, params)

        tries = 5
        for _ in range(tries):
            self.driver.get(prepared.url)

            wait = selenium.webdriver.support.ui.WebDriverWait(self.driver, 30)

            wait.until(
                selenium.webdriver.support.expected_conditions.presence_of_element_located(
                    (
                        selenium.webdriver.common.by.By.CLASS_NAME,
                        "foia-advanced-search-results-wrapper",
                    )
                )
            )

            page = lxml.html.fromstring(self.driver.page_source)
            page.make_links_absolute(search_url)

            (result_table,) = page.xpath(
                "//table[contains(@class, 'foia-advanced-search-results-table-two')]"
            )
            if result_table.xpath("./tbody/tr"):
                break
        else:
            raise ValueError(f"Could not connect to {prepared.url} after {tries} tries")

        keys = result_table.xpath("./thead/tr/th/text()")

        for row in result_table.xpath("./tbody/tr"):
            yield {key: td.text_content() for key, td in zip(keys, row.xpath("./td"))}

    def case_details(self, case_number: str):
        case_url = self.base_url + "/case/" + case_number
        response = self.get(case_url, timeout=30, verify=False)

        page = lxml.html.fromstring(response.text)
        page.make_links_absolute(case_url)

        # Case Name
        details = {}

        try:
            (name,) = page.xpath("//h1[@class='uswds-page-title page-title']/text()")
        except ValueError:
            response.status_code = 404
            raise scrapelib.HTTPError(response)

        details["name"] = name.strip()

        # Basic Details
        basic_info, *tally_elements = page.xpath(
            "//div[@id='block-mainpagecontent']/div[@class='display-flex flex-justify flex-wrap']"
        )

        columns = basic_info.xpath(".//b")

        for header_element in columns:
            header = header_element.text.strip(": ")
            if header == "Case Number":
                case_number = header_element.tail.strip()
                details[header] = case_number
                details["case_type"] = self._case_type(case_number)
            elif header == "Date Filed":
                date_str = header_element.tail.strip()
                details[header] = datetime.datetime.strptime(
                    date_str, "%m/%d/%Y"
                ).date()
            else:
                details[header] = header_element.tail.strip()

        # Tallies
        tallies = []

        for tally_element in tally_elements:
            tally_details = {}
            columns = tally_element.xpath(".//b")

            for header_element in columns:
                header = header_element.text.strip(": ")
                tally_details[header] = header_element.tail.strip()

            tallies.append(tally_details)

        details["tallies"] = tallies

        # Docket
        if "Docket Activity data is not available" not in response.text:
            details["docket"] = self._docket(page, case_number)
        else:
            details["docket"] = []

        related_documents = []
        if "Related Documents data is not available" not in response.text:
            (related_document_header,) = page.xpath(".//h2[text()='Related Documents']")
            document_list = related_document_header.getnext().getnext()
            for doc_link in document_list.xpath(".//a"):
                related_documents.append(
                    {"name": doc_link.text, "url": doc_link.get("href")}
                )
        details["related_documents"] = related_documents

        allegations = []
        if "Allegations data is not available" not in response.text:
            (allegation_header,) = page.xpath(".//h2[text()='Allegations']")
            allegation_list = allegation_header.getnext().getnext()
            for item in allegation_list.xpath(".//li"):
                allegations.append({"allegation": item.text})

        details["allegations"] = allegations

        # Participants
        participants = []
        if "Participants data is not available" not in response.text:
            (participant_table,) = page.xpath(
                "//table[starts-with(@class, 'Participant')]/tbody"
            )

            for row in participant_table.xpath("./tr[not(td[@colspan=3])]"):
                participant_entry = {}

                participant, address, phone = row.xpath("./td")

                participant_entry["type"] = participant.xpath("./b/text()")[0].strip()
                participant_text = [
                    br.tail.strip() if br.tail else ""
                    for br in participant.xpath("./br")
                ]
                participant_entry["subtype"], *participant_text = participant_text
                participant_entry["participant"] = "\n".join(participant_text).strip()
                participant_entry["address"] = "\n".join(
                    line.strip() for line in address.xpath("./text()")
                ).strip()
                participant_entry["phone_number"] = phone.text.strip()

                participants.append(participant_entry)

        details["participants"] = participants

        # Related Cases
        details["related cases"] = [
            {"related_case_number": case_number}
            for case_number in page.xpath(
                "//table[starts-with(@class, 'related-case')]/tbody//a/text()"
            )
        ]

        try:
            (advanced_search_results,) = self.advanced_search(case_number)
        except (ValueError, selenium.common.exceptions.WebDriverException) as exception:
            warnings.warn(exception)
            self.driver.close()
            self.driver.quit()
            self._new_driver()
        else:
            assert case_number == advanced_search_results.pop("Case Number")
            details.update(advanced_search_results)

        return {k.lower().replace(" ", "_"): v for k, v in details.items()}

    def _docket(self, page, case_number):

        response = self.get(
            f"https://www.nlrb.gov/sort-case-decisions-cp/{case_number}/ds_activity+desc/case-docket-activity/ds-activity-date/1?_wrapper_format=drupal_ajax&_wrapper_format=drupal_ajax"
        )

        page_snippet = lxml.html.fromstring(response.json()[3]["data"])

        (docket_table,) = page_snippet.xpath("//table/tbody")

        docket = list(self._parse_docket_table(docket_table))

        return docket

    def _parse_docket_table(self, docket_table: "lxml.html.HmlElement"):
        for row in docket_table.xpath("./tr"):
            docket_entry: typing.Dict[
                str, typing.Union[str, "datetime.date", None]
            ] = {}

            date, document, party = row.xpath("./td")

            date_str = date.text.strip()
            if date_str == "pre 2010":
                docket_entry["date"] = None
            else:
                docket_entry["date"] = datetime.datetime.strptime(
                    date_str, "%m/%d/%Y"
                ).date()
            if len(document):
                (document_link,) = document.xpath("./a")
                docket_entry["document"] = document_link.text.strip()
                docket_entry["url"] = document_link.get("href")
            else:
                docket_entry["document"] = document.text.strip().strip("*")

            docket_entry["issued_by/filed_by"] = (
                party.text.strip() if party.text else None
            )

            yield docket_entry


if __name__ == "__main__":

    s = NLRB()

    result = s.case_details("18-CA-266091")
    filings_url = s.filings(
        case_types=["R", "C"],
        date_start=datetime.date.today() - datetime.timedelta(days=7),
    )
    tallies_url = s.tallies(
        date_start=datetime.date.today() - datetime.timedelta(days=7)
    )

    print(filings_url)
