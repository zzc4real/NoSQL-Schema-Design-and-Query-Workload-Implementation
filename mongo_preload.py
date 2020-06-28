import pymongo

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

    def user_preprocessing(self):
        self.getConnection()
        # change the type of UpVotes from string to int
        # for row in self.users_collection.find({},{'UpVotes':1}):
        #     if row['UpVotes'] == '':
        #         UpVotes = 0
        #     else: UpVotes = int(row['UpVotes'])
        #     self.users_collection.update_one({'_id': row['_id']}, {'$set': {'UpVotes': UpVotes}})

        self.users_collection.aggregate([{'$project': {
            'Id': 1,
            'DisplayName': 1,
            'UpVotes': 1}},
            {'$out': 'user'}
        ])

    def comment_preprocessing(self):
        self.getConnection()
        self.comment_collection.aggregate([{'$project': {
            'Id': 1,
            'PostId': 1,
            'UserId': 1}},
            {'$out': 'comment'}
        ])

    def vote_preprocessing(self):
        self.getConnection()
        self.vote_collection.aggregate([
            {'$match': {'VoteTypeId': '2'}},
            {'$project': {
            'Id': 1,
            'PostId': 1}},
            {'$out': 'vote'}
        ])

    def question_preprocessing(self):
        self.getConnection()
        # change the type of AnswerCount from string to int
        # for row in self.question_collection.find({},{'AnswerCount':1}):
        #     if row['AnswerCount'] == '':
        #          AnswerCount = 0
        #     else: AnswerCount = int(row['AnswerCount'])
        #     self.question_collection.update_one({'_id': row['_id']}, {'$set': {'AnswerCount': AnswerCount}})
        # change the type of CommentCount from string to int
        # for row in self.question_collection.find({},{'CommentCount':1}):
        #     if row['CommentCount'] == '':
        #         CommentCount = 0
        #     else: CommentCount = int(row['CommentCount'])
        #     self.question_collection.update_one({'_id': row['_id']}, {'$set': {'CommentCount': CommentCount}})
        self.question_collection.aggregate([
            {'$match':{'PostTypeId': '1'}},
            {'$project': {
            'Id': 1,
            'OwnerUserId': 1,
            'AC_count': {'$sum':['$AnswerCount', '$CommentCount']},
            'CreationDate': {'$dateFromString':{'dateString':'$CreationDate'}},
            'AcceptedAnswerId': 1,
            'Title': 1,
            'Tags':{'$split':['$Tags', ' ']}}},
            {'$out': 'question'}
        ])

    def answer_preprocessing(self):
        self.getConnection()
        # change the type of CommentCount from string to int
        # for row in self.answer_collection.find({},{'CommentCount':1}):
        #     if row['CommentCount'] == '':
        #         CommentCount = 0
        #     else: CommentCount = int(row['CommentCount'])
        #     self.answer_collection.update_one({'_id': row['_id']}, {'$set': {'CommentCount': CommentCount}})

        self.answer_collection.aggregate([
            {'$lookup': {
                'from': 'question',
                'localField': 'ParentId',
                'foreignField': 'Id',
                'as': 'Question_detail'}},
            {'$match': {'PostTypeId': '2'}},
            {'$project': {
                'Id': 1,
                'ParentId': 1,
                'OwnerUserId': 1,
                'CommentCount': 1,
                'CreationDate': {'$dateFromString': {'dateString': '$CreationDate'}},
                'Title': 1,
                'Tags': '$Question_detail.Tags'}},
            {'$out': 'answer'}
        ])



qd = QueryDemo()
qd.getConnection()
# qd.user_preprocessing()
# qd.question_preprocessing()
# qd.answer_preprocessing()
# qd.comment_preprocessing()
# qd.vote_preprocessing()