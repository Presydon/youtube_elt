import logging

logger = logging.getLogger(__name__)
table = "yt_api"
    

def insert_rows(cur, conn, schema, row):
    try:
        if schema == "staging":
            
            cur.execute(
                f""" INSERT INTO {schema}.{table}(video_id, video_title, upload_date, duration, video_views, likes_count, comments_count)
                VALUES (%(video_id)s, %(title)s, %(published_at)s, %(duration)s, %(view_count)s, %(like_count)s, %(comment_count)s);
                """, row
            )
        else:
            # CORE uses the names already defined in the STAGING table/transformed row
            cur.execute(
                f""" INSERT INTO {schema}.{table}("video_id", "video_title", "upload_date", "duration", "video_type", "video_views", "likes_count", "comments_count")
                VALUES (%(video_id)s, %(video_title)s, %(upload_date)s, %(duration)s, %(video_type)s, %(video_views)s, %(likes_count)s, %(comments_count)s);
                """, row
            )

        conn.commit() # Crucial: Save the changes!
        logger.info(f"Inserted row with Video_ID: {row.get('video_id')}")

    except Exception as e:
        conn.rollback() # Rollback so the next attempt starts fresh
        logger.error(f"Error inserting row with Video_ID: {row.get('video_id')}. Error: {e}")
        raise e
    

def update_rows(cur, conn, schema, row):

    try:
        # staging
        if schema == "staging":
            video_id = 'video_id'
            upload_date = "published_at"
            video_title = "title"
            video_views = "view_count"
            likes_count = "like_count"
            comments_count = "comment_count"

        # core
        else: 
            video_id = "video_id"
            upload_date = "upload_date"
            video_title = "video_title"
            video_views = "video_views"
            likes_count = "likes_count"
            comments_count = "comments_count"

        cur.execute(
            f""" UPDATE {schema}.{table}
                SET "video_title" = %({video_title})s,
                    "video_views" = %({video_views})s, 
                    "likes_count" = %({likes_count})s, 
                    "comments_count" = %({comments_count})s
                WHERE "video_id" = %({video_id})s AND "upload_date" = %({upload_date})s;
                """, row
        )

        conn.commit()

        logger.info(f"""Updated row with Video_ID: {row[video_id]}""")

    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]}. Error: {e}")
        raise e
    

def delete_rows(cur, conn, schema, ids_to_delete):

    try:
        ids_to_delete = f"""({','.join([f"'{id}'" for id in ids_to_delete])}) """

        cur.execute(
            f""" DELETE FROM {schema}.{table}
                WHERE "Video_ID" IN {ids_to_delete};
                """
        )

        conn.commit()
        
        logger.info(f"""Deleted rows with Video_IDs: {ids_to_delete}""")

    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs: {ids_to_delete}. Error: {e}")
        raise e
