import pymongo
import datetime
from collections import Counter

class QueryDemo():

    def __init__(self):
        pass

    def getConnection(self):
        client = pymongo.MongoClient(host='localhost', port=27017)
        db = client['AI']
        self.users_collection = db.get_collection('user')
        self.question_collection = db.get_collection('question')
        self.answer_collection = db.get_collection('answer')
        self.comment_collection = db.get_collection('comment')
        self.vote_collection = db.get_collection('vote')

    # Find the question that attract most discussion.
    def Q1(self,topic_name):
        self.getConnection()
        for item in self.question_collection.aggregate([
    {
        '$lookup': {
            'from': 'answer',
            'localField': 'Id',
            'foreignField': 'ParentId',
            'as': 'answer_detail'
        }
    }, {
        '$unwind': {
            'path': '$Tags'
        }
    }, {
        '$match': {
            'Tags': topic_name
        }
    }, {
        '$project': {
            'Title': 1,
            'AC_count': 1,
            'subdocument': {
                '$arrayElemAt': [
                    '$answer_detail', 0
                ]
            }
        }
    }, {
        '$project': {
            'Title': 1,
            'attration': {
                '$sum': [
                    '$AC_count', '$subdocument.CommentCount'
                ]
            }
        }
    }, {
        '$sort': {
            'attration': -1
        }
    }, {
        '$limit': 1
    }
]):
            print(item)

    # Find the user with the highest upVote number
    def Q2(self,topic_name):
        self.getConnection()
        post_username = ''
        post_upvote = 0
        comment_username = ''
        comment_upvote = 0
        for item in self.question_collection.aggregate([
    {
        '$project': {
            'OwnerUserId': 1,
            'Tags': 1
        }
    }, {
        '$unwind': {
            'path': '$Tags'
        }
    }, {
        '$match': {
            'Tags': topic_name
        }
    }, {
        '$lookup': {
            'from': 'user',
            'localField': 'OwnerUserId',
            'foreignField': 'Id',
            'as': 'user_detail'
        }
    }, {
        '$project': {
            'subdocument': {
                '$arrayElemAt': [
                    '$user_detail', 0
                ]
            },
            'OwnerUserId': 1
        }
    }, {
        '$project': {
            'upvote': '$subdocument.UpVotes',
            'username': '$subdocument.DisplayName',
            'OwnerUserId': 1
        }
    }, {
        '$sort': {
            'upvote': -1
        }
    }, {
        '$limit': 1
    }, {
        '$project': {
            'username': 1,
            'upvote': 1
        }
    }
]):
            print(item)
            post_username = item[u'username']
            post_upvote = item[u'upvote']
#         for item2 in self.question_collection.aggregate([
#     {
#         '$lookup': {
#             'from': 'question',
#             'localField': 'PostId',
#             'foreignField': 'Id',
#             'as': 'question_detail'
#         }
#     }, {
#         '$project': {
#             'UserId': 1,
#             'subdocument': {
#                 '$arrayElemAt': [
#                     '$question_detail', 0
#                 ]
#             }
#         }
#     }, {
#         '$project': {
#             'UserId': 1,
#             'tags': '$subdocument.Tags'
#         }
#     }, {
#         '$unwind': {
#             'path': '$tags'
#         }
#     }, {
#         '$match': {
#             'tags': 'terminology'
#         }
#     }, {
#         '$lookup': {
#             'from': 'user',
#             'localField': 'UserId',
#             'foreignField': 'Id',
#             'as': 'user_detail'
#         }
#     }, {
#         '$project': {
#             'UserId': 1,
#             'sub': {
#                 '$arrayElemAt': [
#                     '$user_detail', 0
#                 ]
#             }
#         }
#     }, {
#         '$project': {
#             'upvote': '$sub.UpVotes',
#             'username': '$sub.DisplayName'
#         }
#     }, {
#         '$sort': {
#             'upvote': -1
#         }
#     }, {
#         '$limit': 1
#     },{
#         '$project': {
#             'username': 1,
#             'upvote': 1
#         }
#     }
# ]):
#             print(item2)
#             comment_username = item2[u'username']
#             comment_upvote = item2[u'upvote']

    # Find the hardest question to be answered
    def Q3(self, topic_name):
        self.getConnection()
        for item in self.question_collection.aggregate([
    {
        '$unwind': {
            'path': '$Tags'
        }
    }, {
        '$match': {
            'Tags': topic_name
        }
    }, {
        '$project': {
            'Title': 1,
            'AcceptedAnswerId': 1,
            'CreationDate': 1
        }
    }, {
        '$lookup': {
            'from': 'answer',
            'localField': 'AcceptedAnswerId',
            'foreignField': 'Id',
            'as': 'answer_detail'
        }
    }, {
        '$project': {
            'Title': 1,
            'CreationDate': 1,
            'subdocument': {
                '$arrayElemAt': [
                    '$answer_detail', 0
                ]
            }
        }
    }, {
        '$project': {
            'Title': 1,
            'CreationDate': 1,
            'ans_date': '$subdocument.CreationDate'
        }
    }, {
        '$project': {
            'Title': 1,
            'duration': {
                '$subtract': [
                    '$ans_date', '$CreationDate'
                ]
            }
        }
    }, {
        '$sort': {
            'duration': -1
        }
    }, {
        '$limit': 1
    }, {
        '$project': {
            'Title': 1
        }
    }
]):
            print(item)

    # Find question whose accept answer has less upvote than other answer
    def Q4(self):
        stf_title = ''
        self.getConnection()
        for item in self.question_collection.aggregate([
    {
        '$project': {
            'Id': 1,
            'Title': 1,
            'AcceptedAnswerId': 1
        }
    }, {
        '$lookup': {
            'from': 'vote',
            'localField': 'AcceptedAnswerId',
            'foreignField': 'PostId',
            'as': 'vote_detail'
        }
    }, {
        '$project': {
            'Id': 1,
            'Title': 1,
            'AcceptedAnswerId': 1,
            'count': {
                '$size': '$vote_detail'
            }
        }
    }, {
        '$lookup': {
            'from': 'answer',
            'localField': 'Id',
            'foreignField': 'ParentId',
            'as': 'answer_detail'
        }
    }, {
        '$unwind': {
            'path': '$answer_detail'
        }
    }, {
        '$project': {
            'Title': 1,
            'count': 1,
            'answerId': '$answer_detail.Id'
        }
    }, {
        '$lookup': {
            'from': 'vote',
            'localField': 'answerId',
            'foreignField': 'PostId',
            'as': 'vote_detail2'
        }
    }, {
        '$project': {
            'Title': 1,
            'count': 1,
            'other_count': {
                '$size': '$vote_detail2'
            }
        }
    }
]):
            # print(item)
            if(item[u'count'] < item[u'other_count']):
                if(item[u'Title'] != stf_title):
                    print(item[u'Title'])
                    stf_title = item[u'Title']

    # Find top 5 topics in a given period
    def Q5(self, start_date, end_date):
        iso_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
        iso_end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
        self.getConnection()
        for item in self.question_collection.aggregate([
            {'$match': {'CreationDate': {'$gte': iso_start_date, '$lte': iso_end_date}}},
            {'$unwind': '$Tags'},
            {'$group': {'_id': '$Tags', 'Users': {'$addToSet': '$OwnerUserId'}}},
            {'$project': {'_id': 0, 'Topic': '$_id', 'Num': {'$size': '$Users'}}},
            {'$sort': {'Num': -1}},
            {'$limit': 5}
        ]):
            print(item)

    # Find top 5 co-author of a given user
    def Q6(self, user_name):
        self.getConnection()
        coauthor = []
        # step 1: get the post_quesId
        for item in self.users_collection.aggregate([
    {
        '$match': {
            'DisplayName': user_name
        }
    }, {
        '$lookup': {
            'from': 'question',
            'localField': 'Id',
            'foreignField': 'OwnerUserId',
            'as': 'question_detail'
        }
    }, {
        '$project': {
            'subdocument': {
                '$arrayElemAt': [
                    '$question_detail', 0
                ]
            }
        }
    }, {
        '$project': {
            'post_quesId': '$subdocument.Id'
        }
    }
]):
            post_quesId = item[u'post_quesId']
            # step 2 get the user name who post answer
            for item in self.users_collection.aggregate([
            {'$match': {'Id': post_quesId}},
            {'$lookup': {
            'from': 'answer',
            'localField': 'Id',
            'foreignField': 'ParentId',
            'as': 'question_detail'}},
            {'$unwind': {'path': '$question_detail'}},
            {'$lookup': {
            'from': 'user',
            'localField': 'question_detail.OwnerUserId',
            'foreignField': 'Id',
            'as': 'string'}},
            {'$project': {'subdocument': {
                '$arrayElemAt': ['$string', 0]}}},
            {'$project': {'ans_username': '$subdocument.DisplayName'}}]):
                coauthor.append(item[u'ans_username'])

            # step 3 get the user name who post comment
            for item in self.users_collection.aggregate([
    {
        '$match': {
            'Id': '5'
        }
    }, {
        '$lookup': {
            'from': 'comment',
            'localField': 'Id',
            'foreignField': 'PostId',
            'as': 'comment_detail'
        }
    }, {
        '$unwind': {
            'path': '$comment_detail'
        }
    }, {
        '$project': {
            'com_userId': '$comment_detail.UserId'
        }
    }, {
        '$lookup': {
            'from': 'user',
            'localField': 'com_userId',
            'foreignField': 'Id',
            'as': 'string'
        }
    }, {
        '$project': {
            'subdocument': {
                '$arrayElemAt': [
                    '$string', 0
                ]
            }
        }
    }, {
        '$project': {
            'com_username': '$subdocument.DisplayName'
        }
    }
]):
                coauthor.append(item[u'com_username'])

        result = Counter(coauthor)
        print(result)










qd = QueryDemo()
# qd.Q1("terminology")
# qd.Q2("terminology")
# qd.Q3("terminology")
# qd.Q4()
# format: 2016-08-02T15:39:14.947+00:00
# qd.Q5("2019-01-01T00:00:00", "2019-10-10T00:00:00")
qd.Q6('baranskistad')




