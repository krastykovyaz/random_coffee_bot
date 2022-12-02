CREATE TABLE users (
row_id Utf8,
tgid Uint64,
username Utf8,
chapter Utf8,
first_name Utf8,
last_name Utf8,
fi Utf8,
role_and_tasks Utf8,
info Utf8, 
photo Utf8,
social_profile Utf8,
status Utf8,
reg_dt datetime,
notification_id Uint64,
state Utf8,
PRIMARY KEY (tgid)
);

CREATE TABLE orders (
row_id Utf8,
tgid Uint64,
order_dt datetime,
meeting_id Utf8,
notification_id Uint64,
PRIMARY KEY (row_id)
);

CREATE TABLE meetings (
row_id Utf8,
meeting_id Utf8,
created_dt datetime,
end_dt datetime,
result Utf8,
notification_id Uint64,
chapter Utf8,
PRIMARY KEY (row_id)
);

CREATE TABLE meetings_user (
row_id Utf8,
meeting_id Utf8,
tgid Uint64,
tgid_partner Uint64,
PRIMARY KEY (row_id)
);

CREATE TABLE messages (
row_id Utf8,
tgid Uint64,
message_dt datetime,
message Utf8,
PRIMARY KEY (row_id)
);

CREATE TABLE messagesfrombot (
row_id Utf8,
tgid Uint64,
message_dt datetime,
message Utf8,
PRIMARY KEY (row_id)
);

CREATE TABLE feedback (
row_id Utf8,
tgid Uint64,
message_dt datetime,
feedback Utf8,
PRIMARY KEY (row_id)
);

CREATE TABLE meeting_reviews (
row_id Utf8,
tgid Uint64,
meeting_id Utf8,
message_dt datetime,
review Utf8,
PRIMARY KEY (row_id)
);

CREATE TABLE rejections (
row_id Utf8,
meeting_id Utf8,
tgid Uint64,
reject_dt datetime,
PRIMARY KEY (row_id)
);