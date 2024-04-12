In the early stage of this project. The initial goal was to understand our data and create a scalable and modular pipeline that contains all stages of our data processing needs—from data ingestion and preprocessing to feature engineering and modeling. This will allow us to easily adapt by incorporating and changing various features, models, and methodologies by iterations. 

Our project aims to identify clients with a high likelihood of becoming private wealth clients. I hypothesize that If a client has data patterns similar to our current PWM clients, this client has the greatest potential for becoming a private wealth client. If that is true, the most straightforward approach is using similarity calculation to find clients who are similar to private wealth clients. 

We have 45926 current private wealth clients, which is a large number. Calculating similarity directly between each non-PWM and PWM client is computationally exhausted. To address this, I've implemented a two-step process:

Clustering: I apply clustering to our PWM clients to summarize these into a smaller number of representative groups or centroids.
Similarity Calculation: We then calculate the similarity of each potential client to these cluster centroids. This method significantly reduces computation by abstracting the data into manageable insights.


