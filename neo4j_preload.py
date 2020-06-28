import pandas as pd
from py2neo import Graph


def load_data():
    db.run("LOAD CSV WITH HEADERS FROM \"file:///Votes.csv\" AS Votes create (a1:Vote {\
    VoteId:toInt(Votes.Id), \
    PostId: toInt(Votes.PostId), \
    VoteTypeId: toInt(Votes.VoteTypeId), \
    UserId: Votes.UserId})")

    db.run("LOAD CSV WITH HEADERS FROM \"file:///Users.csv\" AS \
    Users create (a1:User {UserId:toInt(Users.Id), \
    DisplayName: Users.DisplayName, \
    UpVotes: Users.UpVotes})")

    db.run("LOAD CSV WITH HEADERS FROM \"file:///Posts.csv\" AS Posts create (a1:Post {PostId:toInt(Posts.Id), \
    PostTypeId: toInt(Posts.PostTypeId), \
    AcceptedAnswerId: toInt(Posts.AcceptedAnswerId), \
    CreationDate: Posts.CreationDate, \
    OwnerUserId: toInt(Posts.OwnerUserId), \
    Title: Posts.Title, \
    Tags: split(Posts.Tags,' '), \
    AnswerCount: toInt(Posts.AnswerCount), \
    CommentCount: toInt(Posts.CommentCount),\
    ParentId: toInt(Posts.ParentId) })")

    db.run("LOAD CSV WITH HEADERS FROM \"file:///Tags.csv\" AS Tags create (a1:Tag { \
    TagId:Tags.Id, \
    TagName: Tags.TagName,\
    Count: Tags.Count})")

    db.run("LOAD CSV WITH HEADERS FROM \"file:///Comments.csv\" AS Comments create (a1:Comment { \
    CommentId:toInt(Comments.Id), \
    PostId: toInt(Comments.PostId),\
    UserId: toInt(Comments.UserId)})")

    db.run("Match (p:Post) \
    SET p.CreationDate = datetime(p.CreationDate)")


def create_relationship():
    # user own a post
    db.run("Match (u:User), (p:Post) \
    Where u.UserId = p.OwnerUserId \
    Create (u)-[r:own]->(p)")

    # question points to answers
    db.run("Match (p1:Post), (p2:Post) \
    Where p1.PostId = p2.ParentId \
    Create (p1)-[r:isparent]->(p2)")

    # question points to accepted answer
    db.run("Match (p1:Post), (p2:Post) \
    Where p1.PostId = p2.AcceptedAnswerId \
    Create (p2)-[r:accept]->(p1)")

    # post have topics
    db.run("Match (p:Post), (t:Tag) \
    Where t.TagName in p.Tags \
    Create (p)-[r:haveTag]->(t)")

    # post have vote
    db.run("Match (p:Post), (v:Vote) \
    Where p.PostId = v.PostId \
    Create (p)-[r:havevotes]->(v)")

    # user make comments
    db.run("Match (u:User), (c: Comment) \
    Where u.UserId = c.UserId \
    Create (u)-[r:make]->(c)")

    # comments on post
    db.run("Match (p:Post), (c: Comment) \
    Where p.PostId = c.PostId \
    Create (c)-[r:on]->(p)")


def create_index():
    db.run("create index on :Tag(TagName)")
    db.run("create index on :Post(PostId)")
    db.run("create index on :User(UserId)")
    db.run("create index on :Comment(CommentId)")
    db.run("create index on :Vote(VoteTypeId)")


def delete_all():
    db.run("Match (n) Detach delete n")


mypassword = "zzczzc970216"
db = Graph("bolt://localhost:7687",password=mypassword)
delete_all()
load_data()
create_relationship()
create_index()