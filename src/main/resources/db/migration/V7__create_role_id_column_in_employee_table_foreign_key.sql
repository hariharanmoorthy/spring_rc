ALTER TABLE employee ADD COLUMN role_id BIGINT REFERENCES role(id) on delete cascade on update cascade default null;
