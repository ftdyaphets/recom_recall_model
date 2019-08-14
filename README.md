# recom_recall_model
&nbsp;&nbsp;&nbsp;&nbsp;由于推荐系统召回模型在单机服务器上运行时遇到内存不够的问题，故将相应的模型重新整理优化记录。<br>

服务器配置：8核60g<br>
spark运行模式：local[6]<br>
数据规模：500万的用户评分数据，用户数9万，物品数3万<br>
模型：基于用户相似度的协同过滤u_sim_match，slopeone<br>

&nbsp;&nbsp;&nbsp;&nbsp;用户相似度尝试过用物品倒排计算两两相似、spark分布式矩阵rowMatrix，但优化效果都不好，依然无法运行出结果，
最后使用spark的minhash、随机分桶加lsh实现，在单机上跑出结果。<br>
&nbsp;&nbsp;&nbsp;&nbsp;slopeone尝试过python surprise库实现，但内存直接溢出，spark没有官方slopeone实现，子集用scala、saprk实现的slopeone也没能运行出结果,
最后使用python实现slopeone算法，运行出结果，但所用时间较长，总共花费60个小时。期间尝试过cython、numpy优化，但效果都不够。
cython数据结构在时间上并不比python字典快多少，且还需要额外的环境配置，numpy矩阵乘法效率高，但在构建矩阵的过程开销较大，甚至远远超过计算时间。<br>
&nbsp;&nbsp;&nbsp;&nbsp;记录一下整理优化结果，期待后续的更好优化方式。
