### Q1 Which artist has the most songs in the data set?
```bash
hadoop jar build/libs/Assign3.jar Q1 /hw/analysis.txt /hw/Q1 /hw/Q1sorted
hadoop fs -cat /hw/Q1sorted/part-r-0000
```
<img>
Discussion

<hr>

### Q2 Which artist’s songs are the loudest on average?
```bash
hadoop jar build/libs/Assign3.jar Q2 /hw/analysis.txt /hw/Q2 /hw/Q2sorted
hadoop fs -cat /hw/Q2sorted/part-r-0000
```
<img>
Discussion

<hr>

### Q3 What is the song with the highest hotttnesss (popularity) score?
```bash
hadoop jar build/libs/Assign3.jar Q3 /hw/analysis.txt /hw/Q3 /hw/Q3sorted
hadoop fs -cat /hw/Q3sorted/part-r-0000
```
<img>
Discussion

<hr>

### Q4 Which artist has the highest total time spent fading in their songs?
```bash
hadoop jar build/libs/Assign3.jar Q4 /hw/analysis.txt /hw/Q4 /hw/Q4sorted
hadoop fs -cat /hw/Q4sorted/part-r-0000
```
<img>
Discussion

<hr>

### Q5 What is the longest song(s)? The shortest song(s)? The song(s) of median length?
```bash
hadoop jar build/libs/Assign3.jar Q5 /hw/analysis.txt /hw/Q5 /hw/Q5sorted
hadoop fs -cat /hw/Q5sorted/part-r-0000
```
<img>
Discussion

<hr>

### Q6 What are the 10 most energetic and danceable songs? List them in descending order.
```bash
hadoop jar build/libs/Assign3.jar Q6 /hw/analysis.txt /hw/Q6 /hw/Q6sorted
hadoop fs -cat /hw/Q6sorted/part-r-0000
```
<img>
Discussion

<hr>

### Q7 Create segment data for the average song. Include start time, pitch, timbre, max loudness, max loudness time, and start loudness.
```bash
hadoop jar build/libs/Assign3.jar Q7 /hw/analysis.txt /hw/Q7/<segment_name>
hadoop fs -cat /hw/Q7/<segment_name>/<part-r-00000
```
<img>
Discussion

<hr>

### Q8 Which artist is the most generic? Which artist is the most unique?
```bash
hadoop jar build/libs/Assign3.jar Q8 /hw/analysis.txt /hw/Q8 /hw/Q8sorted
hadoop fs -cat /hw/Q8sorted/part-r-0000
```
<img>
Discussion

<hr>

### Q9 Imagine a song with a higher hotnesss score than the song in your answer to Q3. List thissong’s tempo, time signature, danceability, duration, mode, energy, key, loudness, when it stops fading in, when it starts fading out, and which terms describe the artist who made it. Give both the song and the artist who made it unique names.
```bash
hadoop jar build/libs/Assign3.jar Q9 /hw/Q3sorted/part-r-00000 /hw/Q9 /hw/Q9sorted
hadoop fs -cat /hw/Q9sorted/part-r-0000
```
<img>
Discussion

<hr>

### Q10 
```bash
hadoop jar build/libs/Assign3.jar Q10 /hw/analysis.txt /hw/Q10 /hw/Q10sorted
hadoop fs -cat /hw/Q10sorted/part-r-0000
```
<img>
Discussion

<hr>

### Combine metadata.txt and analysis.txt into one file
```bash
hadoop jar build/libs/Assign3.jar CombineFiles /test/analysis.txt /test/metadata.txt /test/combined.txt
```