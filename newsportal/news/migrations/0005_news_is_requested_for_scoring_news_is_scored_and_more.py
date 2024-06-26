# Generated by Django 5.0.6 on 2024-06-13 23:49

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("news", "0004_alter_news_vectorized_content"),
    ]

    operations = [
        migrations.AddField(
            model_name="news",
            name="is_requested_for_scoring",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="news",
            name="is_scored",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="news",
            name="score",
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name="news",
            name="score_request_id",
            field=models.CharField(default="", max_length=255),
        ),
        migrations.RunSQL(
            sql='ALTER TABLE news ALTER COLUMN score_request_id SET DEFAULT \'\'',
            reverse_sql='ALTER TABLE news ALTER COLUMN score_request_id DROP DEFAULT',
        ),
        migrations.RunSQL(
            sql='ALTER TABLE news ALTER COLUMN is_scored SET DEFAULT FALSE',
            reverse_sql='ALTER TABLE news ALTER COLUMN is_scored DROP DEFAULT',
        ),
        migrations.RunSQL(
            sql='ALTER TABLE news ALTER COLUMN is_requested_for_scoring SET DEFAULT FALSE',
            reverse_sql='ALTER TABLE news ALTER COLUMN is_requested_for_scoring DROP DEFAULT',
        ),
        migrations.RunSQL(
            sql='ALTER TABLE news ALTER COLUMN score SET DEFAULT 0.0',
            reverse_sql='ALTER TABLE news ALTER COLUMN score DROP DEFAULT',
        ),
    ]
