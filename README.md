# 🎵 spotify streaming analytics pipeline
an end-to-end aws pipeline that ingests spotify web api data, transforms it into a scalable s3 data lake, and powers sql analytics (athena) and tableau dashboards for music listening insights.

## 🔨 architecture overview
spotify api → lambda → s3 raw → glue → s3 proc → athena → tableau


### **data flow**
- **lambda**: pulls spotify data daily into s3 raw  
- **glue (crawler + job)**: infers schema, converts json → parquet  
- **athena**: runs sql queries over partitioned parquet datasets  
- **tableau**: connects via csv exports to build dashboards  

---

### **tech stack**
- **aws**: lambda, s3, glue, athena
- **languages/frameworks**: python, pyspark, sql, bash  
- **apis**: spotify web api  
- **data formats**: ndjson, parquet, json/csv
- **bi tools**: tableau  

---

## 📊 analytics examples

### 🎧 track preferences  
*explores listening styles at the track level.*  
- album rollups and comparisons  
- track duration vs. popularity scatter plots  
- release year distribution of favorite music  

📸 

---

### 🎤 music identity  
*artist-level analysis of music taste.*  
- top artists by play count  
- genre composition and distributions  
- popularity vs. follower counts across artists  

📸 

---

### 📈 taste shifts  
*longitudinal view of how preferences evolve.*  
- mainstream vs. niche music trends  
- popularity distributions over time  
- patterns in personal taste evolution  

📸  

---

## 🚀 future enhancements
- **step functions + eventbridge** for orchestration and daily automation  
- **robust error handling**: retries, alerts (sns), and logging  
- **cloudwatch dashboards** for observability and monitoring  
- **quicksight integration** as an alternative bi platform  

---

## 💡 what i learned

### **data engineering**
- designing serverless etl with aws lambda + glue  
- building a data lake with raw/processed zones  
- managing schema evolution and partition strategies for athena  

### **api integration & data quality**
- handling api rate limits and endpoint restrictions  
- adding validation and quality checks into etl  

### **analytics & visualization**
- writing sql for large datasets in athena  
- creating curated views for bi consumption  
- designing dashboards and telling stories with tableau  

### **cloud infrastructure**
- deploying iac with aws sam/cloudformation  
- securing access with iam and secrets manager  

