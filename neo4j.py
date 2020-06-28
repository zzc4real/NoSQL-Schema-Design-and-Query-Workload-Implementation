from py2neo import Graph

# Giving a topic, find the question that attract the most discussion.
def Q1(topic_name):
    return db.run("MATCH (p:Post)-[r:isparent]->(a:Post) where " + topic_name + " in p.Tags RETURN p.Title order by p.CommentCount+p.AnswerCount+a.CommentCount Desc LIMIT 1")


# Giving a topic, find the user with the highest upVote number
# user could make question,answer,comments on corresponding topic
def Q2(topic_name):

    # the user with the highest upVote number who post a question
    query1 = "MATCH (u:User)-[r:own]->(q:Post) where "+ topic_name + " in q.Tags RETURN u.DisplayName, u.UpVotes order by u.UpVotes Desc LIMIT 1"
    user_to_question = db.run(query1).data()[0][u'u.UpVotes']
    user1 = db.run(query1).data()[0][u'u.DisplayName']
    # the user with the highest upVote number who post an answer
    query2 = "MATCH (u:User)-[r1:own]->(a:Post)<-[r2:isparent]-(q:Post) where "+ topic_name + " in q.Tags RETURN u.DisplayName,u.UpVotes order by u.UpVotes Desc LIMIT 1"
    user_to_answer = db.run(query2).data()[0][u'u.UpVotes']
    user2 = db.run(query2).data()[0][u'u.DisplayName']
    # the user with the highest upVote number who post a comment
    query3 = "MATCH (u:User)-[r1:make]->(c:Comment)-[r2:on]->(q:Post) where "+ topic_name + " in q.Tags RETURN u.DisplayName,u.UpVotes order by u.UpVotes Desc LIMIT 1"
    user_to_comment = db.run(query3).data()[0][u'u.UpVotes']
    user3 = db.run(query3).data()[0][u'u.DisplayName']

    if user_to_question >= user_to_answer:
        if user_to_question >= user_to_comment:
            print(user1)
        else: print(user3)
    else:
        if user_to_answer >=user_to_comment:
            print(user2)
        else: print(user3)

# Find the hardest question to be answered
def Q3(topic_name):
    return db.run("MATCH (q:Post)-[r:isparent]->(a:Post) where " + topic_name + " in q.Tags Return q.Title order by duration.between(q.CreationDate,a.CreationDate).seconds Desc LIMIT 1")

# Find question whose accept answer has less upvote than other answer
def Q4():
    return db.run("match ()<-[:accept]-(p:Post)-[:havevotes]-(v:Vote) \
where v.VoteTypeId = 2 \
with p.PostId as pid, count(*) as coun \
match (p1:Post{PostId:pid})-[:isparent]-(p:Post)-[:havevotes]-(v:Vote) \
where v.VoteTypeId = 2 \
with p1.PostId as question ,p.PostId as answer, count(*) as upvotes \
with question, max(upvotes) as max order by question \
match (p1:Post)-[:accept]->(p:Post)-[:havevotes]->(v:Vote) \
where v.VoteTypeId = 2 and p1.PostId =question \
with p1.PostId as question , count(*) as accepted, max order by question \
where accepted <> max \
return question, accepted, max")


# Find top 5 topics in a given period
def Q5(start_date, end_date):
    return db.run("match (q:Post)-[:haveTag]->(t:Tag)\
where timestamp(q.CreationDate) > timestamp('" + start_date + "') and timestamp(q.CreationDate) < timestamp('" + end_date + "')" +
"with count(*) as coun,t.TagName as name \
order by coun desc limit 5 \
return name, coun")


def Q6(userid):
    return db.run("Match (u:User) -[r1:own]->(p1:Post)-[r2:isparent*1..2]-(p2:Post)<-[r3:own]- (u2:User) \
Where u.UserId = "+str(userid)+" and u2.UserId <> "+str(userid)+" \
Return u2.UserId, count(*) as count \
Order by count desc \
limit 5 \
")

mypassword = "zzczzc970216"
db = Graph("bolt://localhost:7687",password=mypassword)

# print("Q1")
# print(Q1("'terminology'").data())

# print("Q2")
# Q2("'terminology'")

# print("Q3")
# print(Q3("'terminology'").data())

# print("Q4")
print(Q4().data())

# print("Q5")
# print(Q5("2019-01-01T00:00:00", "2019-10-01T00:00:00").data())

# print("Q6")
# print(Q6(4398).data())