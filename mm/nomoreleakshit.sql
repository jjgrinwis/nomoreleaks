/* 
Our key to store some metadata about the version of our No More Leaks KV.
If the external script is updating the database, this 'metadata' should be updated, not deleted!
*/
LET key='metadata'

/*
version number of the No More Leaks database used.
set this in the value.active_version field on the metadata key. 
If it doesn't exists set it to the default_version.
version is used to keep track of hits per version of the No More Leaks database.
If active_version is updated, a new key in the value is created.
*/
LET default_version = "1"
LET active_version = DOCUMENT('NoMoreLeaks/metadata').value.active_version ? : default_version

/* a LET just to make query worker script easier to read */
LET version_key = CONCAT("version_", active_version)

/*
If you want to use dynamic vars use [] around your keys!
With upsert we are just going to insert or update if the key exists.
https://www.macrometa.com/docs/queries/c8ql/operations/upsert
if metadata key doesn't exist, create it otherwise only update [version_key] field
*/
UPSERT { _key: key }
    INSERT { _key: key, value: {active_version: active_version, version_updated:DATE_NOW(),[version_key]:PUSH([],DATE_NOW())}}
    UPDATE { value: {[version_key]:PUSH(OLD.value[version_key],DATE_NOW())}}
    IN NoMoreLeaks

/* return the last hit, no need to return complete document */
RETURN {active_version: active_version, added_entry: LAST(NEW.value[version_key])}