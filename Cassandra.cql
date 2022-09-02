CREATE KEYSPACE lambda WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE lambda;
CREATE TABLE activity (
    course_id text,
    user_id text,
    session_id text,
    activity_event text,
    time timestamp,
    PRIMARY KEY (time)
);/* WITH CLUSTERING ORDER BY (time ASC);*/