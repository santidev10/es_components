import json

TOP_LEVEL_CATEGORIES = [
    "automotive",
    "books & literature",
    "business & finance",
    "careers",
    "education",
    "events & attractions",
    "family & relationships",
    "fine art",
    "food & drink",
    "healthy living",
    "hobbies & interests",
    "home & garden",
    "medical health",
    "movies",
    "music & audio",
    "news & politics",
    "personal finance",
    "pets",
    "pop culture",
    "real estate",
    "religion & spirituality",
    "science",
    "shopping",
    "sports",
    "style & fashion",
    "technology & computing",
    "television",
    "travel",
    "video gaming",
    "comedy",
    "nonprofits & activism",
    "kids content",
    "people & blogs"
]

IAB_TO_YOUTUBE_CATEGORIES_MAPPING = {
    "automotive": "Autos & Vehicles",
    "education": "Education",
    "movies": "Film & Animation",
    "music & audio": "Music",
    "news & politics": "News & Politics",
    "pets": "Pets & Animals",
    "pop culture": "Entertainment",
    "sports": "Sports",
    "style & fashion": "Howto & Style",
    "technology & computing": "Science & Technology",
    "travel": "Travel & Events",
    "video gaming": "Gaming",
    "comedy": "Comedy",
    "nonprofits & activism": "Nonprofits & Activism",
    "kids content": "Kids Content",
    "people & blogs": "People & Blogs",
    "television": "Shows"
}

YOUTUBE_TO_IAB_CATEGORIES_MAPPING = {
    'autos & vehicles': ["Automotive"],
    'education': ["Education"],
    'film & animation': ["Movies"],
    'music': ["Music & Audio"],
    'news & politics': ["News & Politics"],
    'pets & animals': ["Pets"],
    'entertainment': ["Pop Culture"],
    'sports': ["Sports"],
    'howto & style': ["Style & Fashion"],
    'science & technology': ["Technology & Computing"],
    'travel & events': ["Travel"],
    'gaming': ["Video Gaming"],
    'comedy': ["Television", "Comedy TV"],
    'nonprofits & activism': ["Business & Finance", "Industries", "Non-Profit Organizations"],
    'kids content': ["Kids Content"],
    'people & blogs': ["Content Channel", "Social"],
    'movies': ["Movies"],
    'shows': ["Television"],
    'trailers': ["Movies"]
}

IAB_TIER2_SET = []
with open("es_components/iab_tier2_categories.json", "r") as f:
    IAB_TIER2_CATEGORIES_MAPPING = json.load(f)

    for tier_1, tier_2 in IAB_TIER2_CATEGORIES_MAPPING.items():
        tier_2 += [tier_1]
        IAB_TIER2_SET.extend(tier_2)
    IAB_TIER2_SET = set(IAB_TIER2_SET)

IAB_TIER3_CATEGORIES_MAPPING = []
with open("es_components/iab_tier3_categories.json", "r") as f:
    IAB_TIER3_CATEGORIES_MAPPING = json.load(f)
