# Generated by Django 5.0.6 on 2024-06-11 16:40

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("news", "0002_alter_news_title_alter_news_table"),
    ]

    operations = [
        migrations.AddField(
            model_name="news",
            name="is_requested_for_summarization",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="news",
            name="is_summarized",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="news",
            name="summarization_request_id",
            field=models.CharField(default="", max_length=255),
        ),
        migrations.RunSQL(
            sql='ALTER TABLE news ALTER COLUMN is_requested_for_summarization SET DEFAULT FALSE',
            reverse_sql='ALTER TABLE news ALTER COLUMN is_requested_for_summarization DROP DEFAULT',
        ),
        migrations.RunSQL(
            sql='ALTER TABLE news ALTER COLUMN is_summarized SET DEFAULT FALSE',
            reverse_sql='ALTER TABLE news ALTER COLUMN is_summarized DROP DEFAULT',
        ),
        migrations.RunSQL(
            sql='ALTER TABLE news ALTER COLUMN summarization_request_id SET DEFAULT \'\'',
            reverse_sql='ALTER TABLE news ALTER COLUMN summarization_request_id DROP DEFAULT',
        ),
    ]
