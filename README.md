Taghreed
========

Taghreed is a a full-fledged system for efficient and scalable querying, analyzing, and visualizing geo-tagged microblogs, e.g., tweets. Taghreed supports arbitrary queries on a large number of microblogs that go up to several months in the past. Taghreed consists of four main components: 

(1) Indexer
(2) Query engine
(3) Recovery manager
(4) Visualizer

Taghreed indexer efficiently digests incoming microblogs with high arrival rates in light memory-resident indexes. When the memory becomes full, a flushing policy manager transfers the memory contents to disk indexes which are managing a large number (Billions) of microblogs for several months. On memory failure, the recovery manager restores the system status from replicated copies for the main-memory content. Taghreed query engine consists of two modules: a query optimizer and a query processor. The query optimizer generates a cheap query plan to be executed by the query processor through efficient retrieval techniques to provide low query response, i.e., order of milli-seconds. Taghreed visualizer then presents the an-
swers so that end users could deal with the system interactively. Taghreed is the first system that addresses all these challenges collectively for microblogs data.
