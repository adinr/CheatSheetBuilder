import os.path
import datetime
import re
import requests
import sys
import csv
import json
import argparse
import logging
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


class CheatSheetBuilder:
    FIELDS = [
        "date",
        "parasha_title",
        "s1",
        "s2",
        "host",
        "shacharit",
        "maftir",
        "torah",
        "parasha_name",
        "parasha_book",
        "parasha_chapter",
        "parasha_verse",
        "parasha_etz_hayim",
        "parasha_hertz",
        "fourth_aliyah_chapter",
        "fourth_aliyah_verse",
        "mi_shebeyrach_s1",
        "seventh_aliyah_chapter",
        "seventh_aliyah_verse",
        "maftir_aliyah",
        "haftarah_parasha",
        "haftarah_book",
        "haftarah_chapter",
        "haftarah_verse",
        "haftarah_etz_hayim",
        "haftarah_hertz",
        "musaf",
        "dvar_torah",
        "next_meeting_date",
        "next_meeting_time",
        "thank_you_shamash",
        "thank_you_shacharit",
        "thank_you_torah",
        "thank_you_haftarah",
        "thank_you_musaf",
        "thank_you_teaching",
        "thank_you_greeter",
        "thank_you_kiddush_volunteer",
        "kiddush_sponsor",
        "scotch_sponsor",
    ]

    ORDINALS = {1: "first", 2: "second", 3: "third", 4: "fourth", 5: "fifth", 6: "sixth", 7: "seventh", 8: "eighth"}

    BOOKS = {
        "Genesis": "B’reshit",
        "Exodus": "Sh’mot",
        "Leviticus": "Vayikra",
        "Numbers": "B’midbar",
        "Deuteronomy": "D’varim",
        "Joshua": "Yehoshua (Joshua)",
        "Judges": "Shof’tim (Judges)",
        "I Samuel": "Sh’muel Aleph (I Samuel)",
        "II Samuel": "Sh’muel Bet (II Samuel)",
        "I Kings" :"M’lachim Aleph (I Kings)",
        "II Kings" :"M’lachim Bet (II Kings)",
        "Isaiah": "Yeshayahu (Isaiah)",
        "Jeremiah": "Yirm’yahu (Jeremiah)",
        "Ezekiel": "Yechezkel (Ezekiel)",
        "Hosea": "Hoshea (Hosea)",
        "Amos": "Amos",
        "Micah": "Mikhah (Micah)",
        "Obadiah": "Ovadiah",
        "Habakkuk": "Chavakkuk",
        "Zechariah": "Zechariah",
        "Malachi": "Malachi",
    }

    # source_documents file should contain these fields
    # TEMPLATE_DOCUMENT_ID
    # ROSH_CHODESH_TEMPLATE_DOCUMENT_ID
    # HANUKKAH_TEMPLATE_DOCUMENT_ID
    # HANUKKAH_ROSH_CHODESH_TEMPLATE_DOCUMENT_ID
    # SHUVA_TEMPLATE_DOCUMENT_ID
    # SHEKALIM_TEMPLATE_DOCUMENT_ID
    # SHEKALIM_ROSH_CHODESH_DOCUMENT_ID
    # ZACHOR_TEMPLATE_DOCUMENT_ID
    # PARAH_TEMPLATE_DOCUMENT_ID
    # HACHODESH_TEMPLATE_DOCUMENT_ID
    # HACHODESH_ROSH_CHODESH_TEMPLATE_DOCUMENT_ID
    # CALENDAR_SHEET_ID
    # KIDDUSH_SHEET_ID
    # PAGE_NUMBERS_SHEET_ID
    # SCOTCH_SHEET_ID

    def __init__(self,
        credentials,
        source_documents,
        shamashim,
        logger,
    ):
        with open(source_documents) as source_documents_fileobj:
            for doc_id_field_name, doc_id in json.load(source_documents_fileobj).items():
                setattr(self, doc_id_field_name, doc_id)

        with open(shamashim) as shamashim_fileobj:
            self.shamashim = json.load(shamashim_fileobj)

        self.logger = logger

        SCOPES = [
            "https://www.googleapis.com/auth/documents",
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive",
        ]
        flow = InstalledAppFlow.from_client_secrets_file(credentials, SCOPES)
        creds = flow.run_local_server(port=0)

        self.docs_service = build("docs", "v1", credentials=creds)
        self.drive_service = build("drive", "v3", credentials=creds)
        self.sheets_service = build("sheets", "v4", credentials=creds)

    def get_document(self, document_id):
        return self.docs_service.documents().get(documentId=document_id).execute()

    def copy_template(self, date, parasha, special):
        title = f"{date.strftime('%Y-%m-%d')} {parasha}"
        document_id = self.TEMPLATE_DOCUMENT_ID
        if special["rosh chodesh"]:
            document_id = self.ROSH_CHODESH_TEMPLATE_DOCUMENT_ID
        if special["hanukkah"]:
            document_id = self.HANUKKAH_ROSH_CHODESH_TEMPLATE_DOCUMENT_ID if special["rosh chodesh"] else self.HANUKKAH_TEMPLATE_DOCUMENT_ID
        if special["shuva"]:
            document_id = self.SHUVA_TEMPLATE_DOCUMENT_ID
        if special["four parshiyot"] == "Shabbat Shekalim":
            document_id = self.SHEKALIM_ROSH_CHODESH_DOCUMENT_ID if special["rosh chodesh"] else self.SHEKALIM_TEMPLATE_DOCUMENT_ID
        if special["four parshiyot"] == "Shabbat Zachor":
            document_id = self.ZACHOR_TEMPLATE_DOCUMENT_ID
        if special["four parshiyot"] == "Shabbat Parah":
            document_id = self.PARAH_TEMPLATE_DOCUMENT_ID
        if special["four parshiyot"] == "Shabbat HaChodesh":
            document_id = self.HACHODESH_ROSH_CHODESH_TEMPLATE_DOCUMENT_ID if special["rosh chodesh"] else self.HACHODESH_TEMPLATE_DOCUMENT_ID
        drive_response = self.drive_service.files().copy(fileId=document_id, body={"name": title}).execute()
        document_copy_id = drive_response.get("id")
        return document_copy_id

    def find_fields_in_content(self, content):
        found_fields = {}
        if "table" in content:
            for row in content["table"]["tableRows"]:
                for cell in row["tableCells"]:
                    for cell_content in cell["content"]:
                        found_fields.update(self.find_fields_in_content(cell_content))
        if "paragraph" in content:
            for element in content["paragraph"]["elements"]:
                if "textRun" not in element:
                    continue
                for field in self.FIELDS:
                    element_content = element["textRun"]["content"]
                    field_placeholder = f"<{field.upper()}>"
                    if field_placeholder in element_content:
                        start_index = element.get("startIndex", 0) + element_content.find(field_placeholder)
                        found_fields[field] = (start_index, start_index + len(field_placeholder))
        return found_fields

    def find_fields(self, document_id):
        document = self.get_document(document_id)
        found_fields = {}
        for content in document["body"]["content"]:
            found_fields.update(self.find_fields_in_content(content))
        return found_fields

    def create_named_ranges(self, document_id):
        found_fields = self.find_fields(document_id)
        document = self.get_document(document_id)
        header = list(document["headers"].values())[0]
        found_fields.update(self.find_fields_in_content(header["content"][0]))
        self.logger.debug(f"missing: {[field for field in self.FIELDS if field not in found_fields]}")
        assert all([field in found_fields for field in self.FIELDS])

        res = self.docs_service.documents().batchUpdate(
            documentId=document_id,
            body={
                "requests": [
                    {
                        "createNamedRange": {
                            "name": field_name,
                            "range": {
                                "startIndex": start_index,
                                "endIndex": end_index,
                                "segmentId": header["headerId"] if field_name == "date" else "",
                            },
                        },
                    }
                    for field_name, (start_index, end_index) in found_fields.items()
                ],
            },
        ).execute()

    def extract_torah_readers(self, raw_data):
        readers = re.findall("[A-Za-z]+ [A-Za-z\\-]*", raw_data)
        return ", ".join(readers)

    def extract_aliyah_leyner(self, raw_data, aliyah_number):
        for line in raw_data.split("\n"):
            ranges = re.findall("(\\d+)-(\\d+)", line)
            for first_aliyah, last_aliyah in ranges:
                if aliyah_number >= int(first_aliyah) and aliyah_number <= int(last_aliyah):
                    return line[line.find(":", line.find(f"{first_aliyah}-{last_aliyah}")) + 1:].split()[0]
            aliyot = re.findall("\\d+", line)
            for aliyah in aliyot:
                if aliyah_number == int(aliyah):
                    return line[line.find(":", line.find(aliyah_number)) + 1:].split()[0]
                    return line.split(":")[1].split()[0].strip()
            if not ranges and not aliyot:
                # single leyner
                return line.split()[0]

    def is_dst_shabbat(self, date):
        return (
            (date.month == 3 and date.day >= 14) or  # Shabbat is after the second Sunday of March,
            (date.month > 3 and date.month < 11) or  # between March and November,
            (date.month == 11 and date.day < 7)      # or before the first Sunday of November
        )

    def get_shamash_full_name_and_email(self, shamash):
        return self.shamashim.get(shamash, (shamash, None))

    def collect_fields_from_calendar(self, date):
        fields = {}
        response = self.sheets_service.spreadsheets().values().get(spreadsheetId=self.CALENDAR_SHEET_ID, range=f"Calendar!A2:L55").execute()
        rows = [row for row in response["values"] if row[0] == date.strftime("%-m/%-d/%Y")]
        assert len(rows) == 1, "Could not find this Shabbat's row in davening calendar (or more than one row?)"
        row = rows[0][:12]
        row += ["" for _ in range(12 - len(row))]  # pad row to contain 12 cells
        fields["parasha_title"] = row[1]
        fields["s1"] = row[9]
        fields["s2"] = row[10]
        fields["host"] = row[11]
        fields["shacharit"] = row[3]
        fields["maftir"] = row[5]
        fields["torah_multiline"] = row[4]
        fields["torah"] = "; ".join(fields["torah_multiline"].split("\n"))
        fields["parasha_name"] = fields["parasha_title"]
        fields["parasha_sixth_and_seventh"] = fields["parasha_title"]  # for 3-torah shabbatot
        fields["maftir_aliyah"] = fields["maftir"]
        fields["haftarah_parasha"] = f"The haftarah for Parashat {fields['parasha_title']}"
        fields["mi_shebeyrach_s1"] = fields["s1"]
        fields["musaf"] = row[6]
        fields["dvar_torah"] = row[7]
        fields["greeter"] = row[8]
        next_meeting_date = date + datetime.timedelta(days=7)
        fields["next_meeting_date"] = next_meeting_date.strftime("%B %-d")
        if self.is_dst_shabbat(next_meeting_date):
            fields["next_meeting_time"] = "9:30"
        else:
            fields["next_meeting_time"] = "9:00"
        fields["thank_you_host"] = self.get_shamash_full_name_and_email(fields["host"])[0]
        shamashim = []
        if fields["host"] not in fields["s1"]:
            shamashim.append(self.get_shamash_full_name_and_email(fields["s1"])[0])
        if fields["host"] not in fields["s2"]:
            shamashim.append(self.get_shamash_full_name_and_email(fields["s2"])[0])
        fields["thank_you_shamash"] = ", ".join(shamashim)
        fields["thank_you_shacharit"] = fields["shacharit"]
        fields["thank_you_torah"] = self.extract_torah_readers(fields["torah_multiline"])
        fields["thank_you_haftarah"] = fields["maftir"]
        fields["thank_you_musaf"] = fields["musaf"]
        fields["thank_you_teaching"] = fields["dvar_torah"]
        fields["thank_you_greeter"] = fields["greeter"]
        return fields

    def collect_fields_from_kiddush_spreadsheet(self, date):
        fields = {}
        response = self.sheets_service.spreadsheets().values().get(spreadsheetId=self.KIDDUSH_SHEET_ID, range="Upcoming!A2:F55").execute()
        rows = [row for row in response["values"] if row and date.strftime("%-m/%-d") in row[0]]
        if not rows:
            self.logger.warning(f"could not find date {date.strftime('%-m/%-d')} in kiddush spreadsheet")
            return fields
        row = rows[0][:5]
        row += ["" for _ in range(5 - len(row))]  # pad row to contain 5 cells
        fields["kiddush_volunteer"] = row[4]
        fields["kiddush_sponsor"] = f"{row[1]}, {row[3]}"
        response = self.sheets_service.spreadsheets().values().get(spreadsheetId=self.SCOTCH_SHEET_ID, range=f"2015+!A2:E55").execute()
        rows = [row for row in response["values"] if row and row[0] == date.strftime("%-m/%-d/%Y")]
        if not rows:
            self.logger.warning(f"could not find date {date.strftime('%-m/%-d/%Y')} in scotch spreadsheet")
            return fields
        row = rows[0][:5]
        row += ["" for _ in range(5 - len(row))]  # pad row to contain 5 cells
        fields["scotch_sponsor"] = f"{row[2]}, {row[4]}"
        fields["thank_you_kiddush_volunteer"] = fields["kiddush_volunteer"]
        return fields

    def collect_leyning_fields(self, date):
        fields = {}
        r = requests.get(f"https://www.hebcal.com/hebcal?v=1&cfg=json&start={date.strftime('%Y-%m-%d')}&end={date.strftime('%Y-%m-%d')}&s=on")
        res = r.json()
        for item in res["items"]:
            if "leyning" not in item:
                continue
            for aliyah, source in item["leyning"].items():
                if aliyah not in ("1", "4", "7", "maftir", "haftarah"):
                    continue
                self.logger.debug((aliyah, source))
                book, start = source.split("-")[0].rsplit(None, 1)
                start_chapter, start_verse = start.split(":")
                if aliyah == "1":
                    fields["parasha_book_english"] = book
                    fields["parasha_book"] = self.BOOKS[book]
                    fields["parasha_chapter"] = start_chapter
                    fields["parasha_verse"] = start_verse
                if aliyah == "4":
                    fields["fourth_aliyah_chapter"] = start_chapter
                    fields["fourth_aliyah_verse"] = start_verse
                if aliyah == "7":
                    fields["seventh_aliyah_chapter"] = start_chapter
                    fields["seventh_aliyah_verse"] = start_verse
                if aliyah == "maftir":
                    # need these only for hanukkah where the maftir varies by day
                    fields["maftir_book_english"] = book
                    fields["maftir_book"] = self.BOOKS[book]
                    fields["maftir_chapter"] = start_chapter
                    fields["maftir_verse"] = start_verse
                    fields["maftir_hanukkah_note"] = start_verse  # ignored on non-hanukkah
                if aliyah == "haftarah":
                    fields["haftarah_book_english"] = book
                    fields["haftarah_book"] = self.BOOKS[book]
                    fields["haftarah_chapter"] = start_chapter
                    fields["haftarah_verse"] = start_verse
        return fields

    def collect_standing_aliyah_leyner_field(self, special, torah_readers):
        fields = {}
        if special["yitro"]:
            fields["ten_commandments_leyner"] = self.extract_aliyah_leyner(torah_readers, 6)
        if special["vaetchanan"]:
            fields["ten_commandments_leyner"] = self.extract_aliyah_leyner(torah_readers, 4)
        if special["last parasha"]:
            fields["last_parasha_leyner"] = self.extract_aliyah_leyner(torah_readers, 7)
        if special["shabbat shira"]:
            fields["shirat_hayam_leyner"] = self.extract_aliyah_leyner(torah_readers, 4)
        return fields


    def collect_special_haftarah_field(self, special):
        fields = {}

        if special["rosh chodesh"]:
            fields["haftarah_parasha"] = "The haftarah for Shabbat Rosh Chodesh"
        if special["machar chodesh"]:
            fields["haftarah_parasha"] = "The haftarah for Machar Chodesh"

        if special["hanukkah"]:
            if special["hanukkah"] == 1:
                fields["haftarah_parasha"] = "The haftarah for the first Shabbat Hanukkah"
            elif special["hanukkah"] == 8:
                fields["haftarah_parasha"] = "The haftarah for the second Shabbat Hanukkah"
            else:
                fields["haftarah_parasha"] = "The haftarah for Shabbat Hanukkah"
            fields["maftir_hanukkah_day"] = self.ORDINALS[special["hanukkah"]]

        if special["four parshiyot"]:
            fields["haftarah_parasha"] = f"The Haftarah for {special['four parshiyot']}"
        if special["shabbat hagadol"]:
            fields["haftarah_parasha"] = "The Haftarah for Shabbat HaGadol"

        if special["rebuke"]:
            fields["haftarah_parasha"] = f"The {self.ORDINALS[special['rebuke']]} haftarah of rebuke read during the weeks preceding Tisha B’av"
        if special["chazon"]:
            fields["haftarah_parasha"] = "The haftarah for Shabbat Chazon"
        if special["nachamu"]:
            fields["haftarah_parasha"] = "The haftarah for Shabbat Nachamu"
        if special["consolation"]:
            fields["haftarah_parasha"] = f"The {self.ORDINALS[special['consolation']]} haftarah of consolation, read during the weeks leading up to Rosh Hashanah,"
            if special["consolation 3 appended to 5"]:
                fields["haftarah_parasha"] = (
                    "We follow the tradition that, when Rosh Chodesh Elul coincides with Shabbat Parashat Re’eh, "
                    "the third haftarah of consolation is appended to the fifth.  This combined haftarah is the same as the one read for Parashat Noach, which"
                )

        if special["shuva"]:
            fields["haftarah_parasha"] = "The Haftarah for Shabbat Shuva"
            if special["shuva"] == "Parashat Vayeilech":
                fields["haftarah_book_english"] = "Micah"
                fields["haftarah_book"] = "Mikhah (Micah)"
                fields["haftarah_chapter"] = "7"
                fields["haftarah_verse"] = "18"
                fields["haftarah_note"] = "Mikhah 7:18-20"
            else:
                fields["haftarah_book_english"] = "Joel"
                fields["haftarah_book"] = "Yoel (Joel)"
                fields["haftarah_chapter"] = "2"
                fields["haftarah_verse"] = "15"
                fields["haftarah_note"] = "Yoel 2:15-27"

        return fields

    def collect_hanukkah_fields(self, special):
        fields = {}
        if special["hanukkah"]:
            fields["maftir_hanukkah_day"] = self.ORDINALS[special["hanukkah"]]
        return fields

    def collect_omer_field(self, special):
        fields = {}
        if special["omer"]:
            fields["omer"] = special["omer"]
        return fields

    def collect_page_numbers(self, book, chapter, verse, parasha=None):
        fields = {}
        maftir = False
        response = self.sheets_service.spreadsheets().values().get(spreadsheetId=self.PAGE_NUMBERS_SHEET_ID, range="A3:K72").execute()
        for row in response["values"]:
            if not row:
                # starting with empty row, maftirs are listed
                maftir = True
            row = row[:11]
            row += ["" for _ in range(11 - len(row))]  # pad row to contain 11 cells
            if row[3] == book and row[4] == chapter and row[5] == verse:
                if maftir:
                    fields["maftir_etz_hayim"] = row[1]
                    fields["maftir_hertz"] = row[2]
                else:
                    fields["parasha_etz_hayim"] = row[1]
                    fields["parasha_hertz"] = row[2]
            if row[8] == book and row[9] == chapter and row[10] == verse and (parasha is None or parasha == row[0]):
                fields["haftarah_etz_hayim"] = row[6]
                fields["haftarah_hertz"] = row[7]
        return fields

    def collect_mi_shebeyrach_list(self):
        response = self.sheets_service.spreadsheets().values().get(spreadsheetId=self.PAGE_NUMBERS_SHEET_ID, range="Mi Shebeyrach!A1:A50").execute()
        return {"mi_shebeyrach_list": "\n".join([row[0] for row in response["values"] if row])}

    def collect_birkat_hachodesh_fields(self, special):
        fields = {}
        if not special["mevarchim"]:
            return fields
        month, days = special["mevarchim"]
        fields["birkat_hachodesh_month"] = month
        if days[0][0] == "Sunday":
            fields["birkat_hachodesh_day"] = "tomorrow"
        else:
            fields["birkat_hachodesh_day"] = f"on {days[0][0]}"
        if len(days) == 2:
            fields["birkat_hachodesh_day"] += f" and {'on ' if days[0][0] == 'Sunday' else ''}{days[1][0]}"
        fields["birkat_hachodesh_day"] += f", B'Yom {days[0][1]}"
        if len(days) == 2:
            fields["birkat_hachodesh_day"] += f" UvYom {days[1][1]}"
        return fields

    def collect_notes_field(self, special):
        fields = {}
        if special["notes"]:
            fields["notes"] = "\n".join(special["notes"])
        return fields

    def collect_fields(self, date, special):
        date_string = date.strftime("%-m/%-d/%Y")
        fields = {}
        fields["date"] = date_string
        fields["family_programming"] = (
            "Tot shabbat will be starting at 10:30 in the classroom on the left, followed by Parent & Me at 11:00, "
            "and junior minyan will be starting at 11:00 in the classroom on the right."
        )
        fields.update(self.collect_fields_from_calendar(date))
        fields.update(self.collect_fields_from_kiddush_spreadsheet(date))
        fields.update(self.collect_leyning_fields(date))
        fields.update(self.collect_standing_aliyah_leyner_field(special, fields["torah_multiline"]))
        fields.update(self.collect_special_haftarah_field(special))
        fields.update(self.collect_hanukkah_fields(special))
        fields.update(self.collect_omer_field(special))
        fields.update(self.collect_birkat_hachodesh_fields(special))
        fields.update(self.collect_notes_field(special))
        # Collect maftir page numbers first so that when maftir conincides with the beginning of a parashah
        # (i.e. Shekalim and Ki Tissa, Parah and Chukat) the maftir page numbers don't override the parashah page numbers
        fields.update(self.collect_page_numbers(
            fields["maftir_book_english"], fields["maftir_chapter"], fields["maftir_verse"],
        ))
        fields.update(self.collect_page_numbers(
            fields["parasha_book_english"], fields["parasha_chapter"], fields["parasha_verse"],
        ))
        specific_parasha = None
        if fields["parasha_title"] == "Noach" or special["consolation 3 appended to 5"]:
            specific_parasha = "Noach"  # make sure we don't use Ki Teitze which starts at the same place as Noach
        if fields["parasha_title"] == "Beha'alotcha":
            specific_parasha = "Beha'alotcha"  # make sure we don't use Hanukkah which starts at the same place
        fields.update(self.collect_page_numbers(
            fields["haftarah_book_english"], fields["haftarah_chapter"], fields["haftarah_verse"], specific_parasha,
        ))
        fields.update(self.collect_mi_shebeyrach_list())
        return fields

    def get_special_shabbat(self, date):
        WEEKDAY_TO_NAMES = {
            0: ("Monday", "Sheni"),
            1: ("Tuesday", "Sh'lishi"),
            2: ("Wednesday", "Revi'i"),
            3: ("Thursday", "Chamishi"),
            4: ("Friday", "Shishi"),
            5: ("Saturday", "Shabbat Kodesh"),
            6: ("Sunday", "Rishon"),
        }

        special = {
            "psalm 27": False,
            "hanukkah": None,
            "mevarchim": None,
            "rosh chodesh": False,
            "ulchaparat pasha": False,
            "machar chodesh": False,
            "omer": None,
            "shabbat shira": False,
            "four parshiyot": None,
            "shabbat hagadol": False,
            "omit av harchamim": False,
            "rebuke": None,
            "chazon": False,
            "nachamu": False,
            "consolation": None,
            "consolation 3 appended to 5": False,
            "shuva": False,
            "vaetchanan": False,
            "yitro": False,
            "last parasha": False,
            "notes": [],
        }
        r = requests.get(f"https://www.hebcal.com/converter?cfg=json&gy={date.year}&gm={date.month}&gd={date.day}&g2h=1")
        res = r.json()
        month, day = res["hm"], res["hd"]

        if month == "Elul" or (month == "Tishrei" and day <= 21):  # Elul through Hoshana Rabba
            special["psalm 27"] = True

        # Handle dates with no Av HaRachamim
        if any((
            month == "Nisan",
            month == "Iyyar" and day in (14, 18),
            month == "Sivan" and day >= 6 and day <= 12,
            month == "Av" and day == 15,
            month == "Tishrei" and day >= 9,
            month == "Sh'vat" and day == 15,
            month in ("Adar", "Adar I", "Adar II") and day in (14, 15),
        )):
            special["omit av harchamim"] = True

        # Handle Shabbat Mevarchim
        rosh_chodesh_days = []
        tomorrow = date + datetime.timedelta(days=1)
        next_sunday = date + datetime.timedelta(days=8)
        r = requests.get(f"https://www.hebcal.com/converter?cfg=json&start={tomorrow.strftime('%Y-%m-%d')}&end={next_sunday.strftime('%Y-%m-%d')}&g2h=1")
        coming_week_res = r.json()
        for upcoming_date_str, upcoming_date_data in coming_week_res["hdates"].items():
            upcoming_date = datetime.date.fromisoformat(upcoming_date_str)
            if upcoming_date == next_sunday and not rosh_chodesh_days:
                # ignore rosh chodesh on next Sunday if Shabbat isn't also rosh chodesh (because shabbat mevarchim will be next week)
                break
            for upcoming_day_event in upcoming_date_data["events"]:
                m = re.search("Rosh Chodesh (.*)", upcoming_day_event, re.IGNORECASE)
                if m:
                    weekday = upcoming_date.weekday()
                    month = m.group(1)
                    rosh_chodesh_days.append(WEEKDAY_TO_NAMES[weekday])
                    if upcoming_date == tomorrow:
                        assert weekday == 6
                        special["machar chodesh"] = True
        if rosh_chodesh_days:
            if month == "Adar I":
                month = "Adar Rishon"
            if month == "Adar II":
                month = "Adar Sheni"
            special["mevarchim"] = (month, rosh_chodesh_days)
            self.FIELDS += ["birkat_hachodesh_month", "birkat_hachodesh_day"]
            special["omit av harchamim"] = month not in ("Iyyar", "Sivan")

        for event in res["events"]:
            # Handle Hanukkah
            m = re.search("Chanukah Day (\\d+)", event, re.IGNORECASE)
            if m:
                special["hanukkah"] = int(m.group(1))
                special["omit av harchamim"] = True
                self.FIELDS += ["maftir_hanukkah_day", "maftir_book", "maftir_chapter", "maftir_verse", "maftir_etz_hayim", "maftir_hertz", "maftir_hanukkah_note"]

            # Handle Rosh Chodesh
            if "Rosh Chodesh" in event:
                special["rosh chodesh"] = True
                if res["hy"] % 19 in [0, 3, 6, 8, 11, 14, 17] and month in ["Cheshvan", "Kislev", "Tevet", "Sh'vat", "Adar I", "Adar II"]:
                    special["ulchaparat pasha"] = True
                special["omit av harchamim"] = True

            # Handle Omer
            if "Omer" in event:
                special["omer"] = event  # e.g. "21st day of the Omer"
                self.FIELDS.append("omer")

            # Handle Shabbat Shira
            if event == "Shabbat Shirah":
                special["shabbat shira"] = True
                special["notes"].append("Shabbat Shira")
                self.FIELDS.append("shirat_hayam_leyner")

            # Handle four parshiyot
            if event in ("Shabbat Parah", "Shabbat HaChodesh", "Shabbat Zachor", "Shabbat Shekalim"):
                special["four parshiyot"] = event
                special["omit av harchamim"] = True
            if event == "Shabbat HaGadol":
                special["shabbat hagadol"] = True
                special["omit av harchamim"] = True
                special["notes"].append("Shabbat HaGadol (different haftarah, no Av HaRachamim)")

            # Handle rebuke haftarot
            if month == "Tamuz" and day >= 19 and day <= 24:
                special["rebuke"] = 1
            if (month == "Tamuz" and day >= 26) or (month == "Av" and day <= 2):
                special["rebuke"] = 2
            if event == "Shabbat Chazon":
                special["chazon"] = True
                special["notes"].append("1st Aliyah: ends at 1:10. 2nd Aliyah: begins at 1:11. 1:12 in Eycha Trope")

            # Handle consolation haftarat
            if event == "Shabbat Nachamu":
                special["nachamu"] = True
            if "Eikev" in event:
                special["consolation"] = 2
            if "Re'eh" in event:
                special["consolation"] = 3
            if "Shoftim" in event:
                special["consolation"] = 4
            if "Ki Teitzei" in event:
                special["consolation"] = 5
                if month == "Av" and day == 15:
                    special["consolation 3 appended to 5"] = True
            if "Ki Tavo" in event:
                special["consolation"] = 6
                special["notes"].append("Aliyah 6: Tokhekhah. Aliyah to Ba’al Koreh")
            if "Nitzavim" in event:
                special["consolation"] = 7
            if event == "Shabbat Shuva" and month == "Tishrei" and day >= 1 and day <= 10:
                special["shuva"] = event
                self.FIELDS.append("haftarah_note")

            # Handle Aseret HaDibrot
            if "Vaetchanan" in event:
                special["vaetchanan"] = True
                special["notes"] += ["4th Aliyah: ends at 5:18, and contains Aseret HaDibrot", "5th Aliyah: begins at 5:19"]
                self.FIELDS.append("ten_commandments_leyner")
            if "Yitro" in event:
                special["yitro"] = True
                special["notes"].append("6th Aliyah contains Aseret HaDibrot")
                self.FIELDS.append("ten_commandments_leyner")

            # Handle last parasha in sefer
            if any(parasha in event for parasha in ["Vayechi", "Pekudei", "Bechukotai", "Masei"]):
                special["last parasha"] = True
                self.FIELDS.append("last_parasha_leyner")

            # Handle special aliyot
            if "Bereshit" in event:
                special["notes"].append("6th Aliyah begins with 5:1, not 4:23")
            if "Bechukotai" in event:
                special["notes"].append(f"Aliyah {'5' if 'Behar' in event else '3'}: Tokhekhah. Aliyah to Ba’al Koreh")
            if "Terumah" in event:
                special["notes"].append("3rd Aliyah begins with 26:1, not 25:31")

        if special["rosh chodesh"] and (special["four parshiyot"] or special["hanukkah"]):
            self.FIELDS.append("parasha_sixth_and_seventh")
        if special["notes"]:
            self.FIELDS.append("notes")
        return special

    def get_date(self):
        now = datetime.date.today()
        days_until_next_shabbat = ((4 - now.weekday()) % 7) + 1
        date = now + datetime.timedelta(days=days_until_next_shabbat)
        return date

    def fill_in_fields(self, document_id, fields):
        self.docs_service.documents().batchUpdate(
            documentId=document_id,
            body={
                "requests": [
                    {"replaceNamedRangeContent": {"namedRangeName": field_name, "text": field_value}}
                    for field_name, field_value in fields.items()
                    if field_value
                ]
            },
        ).execute()
        remaining_fields = self.find_fields(document_id)
        if remaining_fields:
            self.docs_service.documents().batchUpdate(
                documentId=document_id,
                body={
                    "requests": [
                        {
                            "updateTextStyle": {
                                "range": {"startIndex": start_index, "endIndex": end_index},
                                "textStyle": {"backgroundColor": {"color": {"rgbColor": {"red": 1}}}},
                                "fields": "backgroundColor"
                            }
                        }
                        for field_name, (start_index, end_index) in remaining_fields.items()
                    ] + [
                        {"replaceNamedRangeContent": {"namedRangeName": field_name, "text": "????"}}
                        for field_name, (start_index, end_index) in remaining_fields.items()
                    ]
                },
            ).execute()

    def update_av_harachamim(self, document_id, special):
        if not special["omit av harchamim"] and not special["mevarchim"]:
            return
        document = self.get_document(document_id)
        for content in document["body"]["content"]:
            if "paragraph" not in content:
                continue
            for element in content["paragraph"]["elements"]:
                if "textRun" not in element:
                    continue
                if "Av HaRachamim" in element["textRun"]["content"]:
                    av_harachamim_start_index, av_harachamim_end_index = element["startIndex"], element["endIndex"]
        if special["omit av harchamim"]:
            requests = [
                {
                    "updateTextStyle": {
                        "range": {"startIndex": av_harachamim_start_index, "endIndex": av_harachamim_end_index},
                        "textStyle": {"foregroundColor": {"color": {"rgbColor": {"red": 1}}}, "strikethrough": True},
                        "fields": "foregroundColor,strikethrough",
                    },
                },
            ]
        else:
            requests = [
                {"insertText": {"location": {"index": av_harachamim_start_index}, "text": "+ "}},
                {
                    "updateTextStyle": {
                        "range": {"startIndex": av_harachamim_start_index, "endIndex": av_harachamim_start_index + len("+ Av HaRachamim")},
                        "textStyle": {"foregroundColor": {"color": {"rgbColor": {"red": 0, "green": 0.6901961, "blue": 0.3137255}}}},
                        "fields": "foregroundColor",
                    },
                },
            ]
        self.docs_service.documents().batchUpdate(documentId=document_id, body={"requests": requests}).execute()

    def delete_section(self, document_id, marker, delete):
        document = self.get_document(document_id)
        begin_start_index, begin_end_index, end_start_index, end_end_index = None, None, None, None
        for content in document["body"]["content"]:
            if "paragraph" not in content:
                continue
            for element in content["paragraph"]["elements"]:
                if "textRun" not in element:
                    continue
                if f"<BEGIN_{marker}>" in element["textRun"]["content"]:
                    begin_start_index, begin_end_index = element["startIndex"], element["endIndex"]
                if f"<END_{marker}>" in element["textRun"]["content"]:
                    end_start_index, end_end_index = element["startIndex"], element["endIndex"]
        if not all([begin_start_index, begin_end_index, end_start_index, end_end_index]):
            return
        if delete:
            # delete the entire section
            ranges_to_delete = [(begin_start_index, end_end_index)]
        else:
            # delete just the markers
            ranges_to_delete = [(end_start_index, end_end_index), (begin_start_index, begin_end_index)]
        self.docs_service.documents().batchUpdate(
            documentId=document_id,
            body={
                "requests": [
                    {"deleteContentRange": {"range": {"startIndex": start_index, "endIndex": end_index}}}
                    for start_index, end_index in ranges_to_delete
                ]
            }
        ).execute()

    def print_document(self, document_id):
        document = self.get_document(document_id)
        for content in document["body"]["content"][1:]:
            self.logger.debug(content["paragraph"]["elements"])

def main():
    parser = argparse.ArgumentParser(
        prog="CheatSheetBuilder",
        description="Generates Cheat Sheets for KHSZ",
    )
    parser.add_argument("-c", "--credentials", help="specify the location of the credentials file")
    parser.add_argument("-i", "--source_docs", help="specify the location of the source documents file")
    parser.add_argument("-s", "--shamashim", help="specify the location of the shamashim file")
    parser.add_argument("-d", "--date", help="specify the date (YYYY-MM-DD) for which to generate the cheat sheet")
    parser.add_argument("-v", "--verbose", action="store_true", help="print debug logs")
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stderr)
    logger = logging.getLogger("CheatSheetBuilder")
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    builder = CheatSheetBuilder(
        args.credentials or "cheat_sheet_credentials.json",
        args.source_docs or "source_documents.json",
        args.shamashim or "shamashim.json",
        logger,
    )
    date = builder.get_date()
    if args.date:
        date = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    special = builder.get_special_shabbat(date)
    fields = builder.collect_fields(date, special)
    document_id = builder.copy_template(date, fields["parasha_title"], special)
    logger.info(f"Generated document: https://docs.google.com/document/d/{document_id}/edit")
    logger.debug(special)
    builder.delete_section(document_id, "OMER", not special["omer"])
    builder.delete_section(document_id, "PSALM_27", not special["psalm 27"])
    builder.delete_section(document_id, "ULCHAPARAT_PASHA_NOTE", not special["ulchaparat pasha"])
    builder.delete_section(document_id, "ULCHAPARAT_PASHA", not special["ulchaparat pasha"])
    builder.delete_section(document_id, "BIRKAT_HACHODESH", not special["mevarchim"])
    builder.delete_section(document_id, "LAST_PARASHA", not special["last parasha"])
    builder.delete_section(document_id, "SHIRAT_HAYAM", not special["shabbat shira"])
    builder.delete_section(document_id, "TEN_COMMANDMENTS_YITRO", not special["yitro"])
    builder.delete_section(document_id, "TEN_COMMANDMENTS_VAETCHANAN", not special["vaetchanan"])
    builder.delete_section(document_id, "NOTES", not special["notes"])
    builder.update_av_harachamim(document_id, special)
    builder.create_named_ranges(document_id)
    builder.fill_in_fields(document_id, fields)
    #builder.drive_service.files().delete(fileId=document_id).execute()
    #builder.drive_service.files().update(fileId=document_id, addParents="10WjWDCfxuOHTI8FGjuqOqImJ0tAtR3nS").execute()

if __name__ == '__main__':
    main()

