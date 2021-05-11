import os
import pandas
import re

from csv import reader
from polyglot.detect import Detector
from polyglot.detect.base import logger as polyglot_logger

from es_components.constants import Sections
from es_components.managers.video_language import VideoLanguageManager


polyglot_logger.disabled = True


def _get_exclusion_list():
    exclusion_list = list()
    exclusion_list_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                            "lang_detection_exclusion_list.csv")
    with open(exclusion_list_file_path, "r") as ex:
        ex_reader = reader(ex)
        for word in ex_reader:
            exclusion_list.append(word[0])
    exclusion_list = exclusion_list[1:]
    return exclusion_list


def _clean_text(text, caps=False, exclusion=True):
    exclusion_list = _get_exclusion_list()
    clean = re.sub('@\\w+', ' ', text)
    clean = re.sub('https?://([^ ]+)', ' ', clean)
    clean = re.sub('www([^ ]+)', ' ', clean)
    clean = re.sub('#\\w*', ' ', clean)
    clean = re.sub('[\n\r]', ' ', clean)
    clean = re.sub('[^\w\s]', ' ', clean)
    clean = re.sub('\\d+\\w*\\d*', ' ', clean)

    # clean = re.sub('^.{10,}$', ' ',clean)
    clean = re.sub('([a-zA-Z]+_[a-zA-Z]+)', ' ', clean)

    if caps:
        clean = re.sub('[A-Z]\\w+', '', clean)
    else:
        clean = clean.lower()

    if exclusion:
        for ex in exclusion_list:
            clean = re.sub(ex, ' ', clean)
    clean = re.sub('\\b[a-zA-Z0-9]{1,2}\\b', ' ', clean)
    clean = re.sub('\\s+', ' ', clean)
    return clean


def _detect_lang_table(detector_object):
    name_detected = list()
    code_detected = list()
    conf_detected = list()
    byte_detected = list()
    for lang in detector_object.languages:
        name_detected.append(lang.name)
        code_detected.append(lang.code)
        conf_detected.append(lang.confidence)
        byte_detected.append(lang.read_bytes)
    output = pandas.DataFrame(dict(name=name_detected,
                                   code=code_detected,
                                   conf=conf_detected,
                                   byte=byte_detected))
    output["prop"] = round(output["byte"] / output["byte"].sum() * 100, 1)
    return output


def _detect_language(text):
    result = dict(is_reliable=False, detected_languages=[])
    if isinstance(text, str) and len(text) > 0:
        clean_text = _clean_text(text)
        detector_obj = Detector(clean_text, quiet=True)
        result["is_reliable"] = detector_obj.reliable
        detected_data_table = _detect_lang_table(detector_obj)
        detected_data_table = detected_data_table.sort_values(
                                    by=["conf", "prop"], ascending=[False, False], ignore_index=True)
        for i in range(3):
            if len(detected_data_table["code"]) > i and detected_data_table["code"][i] != "un":
                detected_language = dict(name=detected_data_table["name"][i],
                                         code=detected_data_table["code"][i],
                                         confidence=detected_data_table["conf"][i])
                result["detected_languages"].append(detected_language)
    return result


def _calculate_video_language_data(video_id=None, video_title=None, video_description=None):
    """
    This function will calculate and return the VideoLanguage object without upserting it
    Also, this function holds the algorithm to detect a video language
    """
    result = None
    if video_id and (video_title or video_description):
        video_lang_mgr = VideoLanguageManager(
            sections=(Sections.GENERAL_DATA, Sections.TITLE_LANG_DATA, Sections.DESCRIPTION_LANG_DATA))

        video_language_object = video_lang_mgr.get_or_create(ids=[video_id])[0]

        # detect the language of the video title
        title_lang_data = dict(is_reliable=False, items=[])
        if video_title:
            detected_title_language = _detect_language(video_title)
            title_lang_data["is_reliable"] = detected_title_language["is_reliable"]
            for language in detected_title_language["detected_languages"]:
                detected_language = dict(lang_name=language["name"], lang_code=language["code"],
                                         confidence=language["confidence"])
                title_lang_data["items"].append(detected_language)
        video_language_object.populate_title_lang_data(**title_lang_data)

        # detect the language of the video description
        description_lang_data = dict(is_reliable=False, items=[])
        if video_description:
            detected_description_language = _detect_language(video_description)
            description_lang_data["is_reliable"] = detected_description_language["is_reliable"]
            for language in detected_description_language["detected_languages"]:
                detected_language = dict(lang_name=language["name"], lang_code=language["code"],
                                         confidence=language["confidence"])
                description_lang_data["items"].append(detected_language)
        video_language_object.populate_description_lang_data(**description_lang_data)

        # if we detect at least one language, do the calculation to determine the primary video language
        if len(title_lang_data["items"]) > 0 or len(description_lang_data["items"]) > 0:
            video_language_general_data = dict()
            video_lang_source_is_title = False

            if ((title_lang_data["is_reliable"] and
                 not description_lang_data["is_reliable"] and
                 len(title_lang_data["items"]) > 0) or (len(title_lang_data["items"]) > 0 and
                                                        len(description_lang_data["items"]) == 0)):
                video_lang_source_is_title = True
                video_language_general_data["primary_lang_details"] = title_lang_data["items"][0]
            else:
                video_language_general_data["primary_lang_details"] = description_lang_data["items"][0]

            # the English language special case:
            # if detected language is english and conf < 99% then choose secondary as primary language (if available)
            if video_language_general_data["primary_lang_details"]["lang_code"] == "en" and \
                    video_language_general_data["primary_lang_details"]["confidence"] < 99:
                if video_lang_source_is_title and title_lang_data["is_reliable"] and \
                        len(title_lang_data["items"]) > 1 and title_lang_data["items"][1]["lang_code"] != "en":
                    video_language_general_data["primary_lang_details"] = title_lang_data["items"][1]
                elif len(description_lang_data["items"]) > 1 and description_lang_data["items"][1]["lang_code"] != "en":
                    video_language_general_data["primary_lang_details"] = description_lang_data["items"][1]

            video_language_object.populate_general_data(**video_language_general_data)
        result = video_language_object
    return result


def detect_video_language(video_id=None, video_title=None, video_description=None):
    """
    This function detects the language of a YT Video based on its title and description. It also upserts a VideoLanguage
    document in ES to keep the language(s) it detected to be used in channel language detection.
    This function returns a string of the primary language code of the video e.g. "en"
    """
    result = ""
    video_lang_obj = _calculate_video_language_data(video_id=video_id, video_title=video_title,
                                                    video_description=video_description)
    if video_lang_obj and video_lang_obj.general_data and video_lang_obj.general_data.primary_lang_details and \
            isinstance(video_lang_obj.general_data.primary_lang_details.lang_code, str):
        result = video_lang_obj.general_data.primary_lang_details.lang_code
    return result
