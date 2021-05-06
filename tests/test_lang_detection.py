import itertools

from unittest.mock import patch
from unittest.mock import Mock

from es_components.models import Video
from es_components.managers import VideoLanguageManager
from es_components.tests.utils import ESTestCase
from es_components.lang_detection import detect_video_language


int_iterator = itertools.count(1, 1)


class LanguageDetectionTestCase(ESTestCase):
    def test_upsert_video_language(self):
        video = Video(f"video_{next(int_iterator)}")
        video.general_data.title = "testing some title in english"
        video.general_data.description = "here is some description also in english for the video"
        video_lang_obj_mock = Mock()

        with patch.object(VideoLanguageManager, "upsert", return_value=video_lang_obj_mock) as mock_upsert:
            detected_lang = detect_video_language(video_id=video.main.id, video_title=video.general_data.title,
                                                  video_description=video.general_data.description)
            self.assertEqual(mock_upsert.call_count, 1)
            self.assertEqual(detected_lang, "en")

    def test_detecting_spanish_language(self):
        video = Video(f"video_{next(int_iterator)}")
        video.general_data.title = "HACEMOS UN EQUIPO DE LA LIGA CON 300 MILLONES | ft. MIGUEL QUINTANA"
        video.general_data.description = "Hacemos nuestros equipos de La Liga con 300M. Jugadorazos #Messi\
                                            , #Ramos, #Suárez, Benzema... ¿Qué once os gusta más?"
        video_lang_obj_mock = Mock()

        with patch.object(VideoLanguageManager, "upsert", return_value=video_lang_obj_mock):
            detected_lang = detect_video_language(video_id=video.main.id, video_title=video.general_data.title,
                                                  video_description=video.general_data.description)
            self.assertEqual(detected_lang, "es")

    def test_detecting_italian_language(self):
        video = Video(f"video_{next(int_iterator)}")
        video.general_data.title = "Sousa saluta: \"Grazie Firenze mia\"- Giornata 38 - Serie A TIM 2016/17"
        video.general_data.description = "Il tecnico della Fiorentina spiega i motivi del suo addio ai viola"
        video_lang_obj_mock = Mock()

        with patch.object(VideoLanguageManager, "upsert", return_value=video_lang_obj_mock):
            detected_lang = detect_video_language(video_id=video.main.id, video_title=video.general_data.title,
                                                  video_description=video.general_data.description)
            self.assertEqual(detected_lang, "it")

    def test_detecting_arabic_language(self):
        video = Video(f"video_{next(int_iterator)}")
        video.general_data.title = "صورة بنصف مليون"
        video.general_data.description = "زوي روث المعروفة بفتاة الكوارث، صاحبة الابتسامة الساخرة في عشرات صور الحوادث"
        video_lang_obj_mock = Mock()

        with patch.object(VideoLanguageManager, "upsert", return_value=video_lang_obj_mock):
            detected_lang = detect_video_language(video_id=video.main.id, video_title=video.general_data.title,
                                                  video_description=video.general_data.description)
            self.assertEqual(detected_lang, "ar")
