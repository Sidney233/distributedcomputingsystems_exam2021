# Twitter Hot Topics

## 难易程度:  \*\* 中

## 待完成:

* 请在DSPPCode.flink.twitter_hot_topics中创建TwitterHotTopicsImpl, 继承TwitterHotTopics, 实现抽象方法
* 请在DSPPCode.flink.twitter_hot_topics中创建TwitterTextHandlerImpl, 继承TwitterTextHandler, 实现抽象方法
## 题目描述:

* 要求：

  来自世界各地的Twitter用户们每时每刻都在发布着新的推文。往往，这些推文所讨论的话题是各不相同的，并且在讨论热度上也是有所差异的。一般来说，弄清哪些话题是热门话题有利于为用户做内容推荐。假设存在一个程序正在源源不断地采集着Twitter的推文数据，现在希望你能对这些数据进行处理，以找出**英文推文中的**热门话题。为了简单起见，推文中的每个**英文单词**被视作一个话题。当话题数大于等于**4**时，该话题称之为热门话题。此外，对于每个话题而言，只有其对应的英文单词具备一定的意义才会纳入热门话题。例如，对于`the`话题，由于`the`无意义，则不应纳入热门话题。为了方便判定话题是否具备一定的意义，本题提供了一份包含所有无意义单词的停词表，可通过`StopWordsOperation.getStopWords`方法获取。

* 输入格式

  每条输入数据的格式为JSON。在每条JSON数据中，"text"字段表示用户发布的推文 ，而"lang"字段表示用户发布的推文所使用的语言。假设，某一Twitter用户发布了**五条相同的英文推文**，其内容均为`how to use flink? i love it`，则推文对应的数据格式如下所示。其中，"text"字段所对应的值为`how to use flink? i love it`，而"lang"字段所对应的值为`en`。

  ```json
  {
    "created_at": "Mon Jan 1 00:00:00 +0000 1901",
    "id": 0,
    "id_str": "000000000000000000",
    "text": "how to use flink? i love it",
    "source": null,
    "truncated": false,
    "in_reply_to_status_id": null,
    "in_reply_to_status_id_str": null,
    "in_reply_to_user_id": null,
    "in_reply_to_user_id_str": null,
    "in_reply_to_screen_name": null,
    "user": {
      "id": 0,
      "id_str": "0000000000",
      "name": "test1",
      "screen_name": "iphone",
      "location": "Shanghai",
      "protected": false,
      "verified": false,
      "followers_count": 999999,
      "friends_count": 99999,
      "listed_count": 999,
      "favourites_count": 9999,
      "statuses_count": 999,
      "created_at": "Mon Jan 1 00:00:00 +0000 1901",
      "utc_offset": 7200,
      "time_zone": "Amsterdam",
      "geo_enabled": false,
      "lang": "en",
      "entities": {
        "hashtags": [
          {
            "text": "example1",
            "indices": [
              0,
              0
            ]
          },
          {
            "text": "tweet1",
            "indices": [
              0,
              0
            ]
          }
        ]
      },
      "contributors_enabled": false,
      "is_translator": false,
      "profile_background_color": "C6E2EE",
      "profile_background_tile": false,
      "profile_link_color": "1F98C7",
      "profile_sidebar_border_color": "FFFFFF",
      "profile_sidebar_fill_color": "252429",
      "profile_text_color": "666666",
      "profile_use_background_image": true,
      "default_profile": false,
      "default_profile_image": false,
      "following": null,
      "follow_request_sent": null,
      "notifications": null
    },
    "geo": null,
    "coordinates": null,
    "place": null,
    "contributors": null
  }
  ```

* 输出格式

  每行以元组形式输出热门话题（全部小写）以及热门话题出现的次数，并且当话题数大于等于4时即开始输出。以上述的五条推文数据为例，love这一热门话题总共出现了5次，因此会产生两次输出。

  ```
  (love,4)
  (love,5)
  (flink,4)
  (flink,5)
  ```

  