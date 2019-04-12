### 贝叶斯模型使用与示例

----

spark 机器学习练习 —— 贝叶斯

#### 数据集来源
  *  数据集来源： http://archive.ics.uci.edu/ml/datasets/Adult

#### 数据集分类结果
- 根据人口普查数据预测年收入是否超过5万美元/年。

#### 数据集说明
  -  第一列：年龄（连续）
  -  第二列：工作类型（离散）Private、 Self - emp - not - inc、 Self - emp - inc、 Federal - gov、 Local - gov、 State - gov、 Without - pay、 Never - worked
  -  第三列：一个州内数据观察代表的人数（连续）
  -  第四列：学历（离散）Bachelors、 Some-college、 11th、 HS-grad、 Prof-school、 Assoc-acdm、 Assoc-voc、 9th、 7th-8th、 12th、 Masters、 1st-4th、 10th、 Doctorate、 5th-6th、 Preschool
  -  第五列：education-num（连续）
  -  第六列：婚姻状况（离散）Married - civ - spouse、 Divorced、 Never - married、 Separated、 Widowed、 Married - spouse - absent、 Married - AF - spouse
  -  第七列：职业（离散）Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces.
  -  第八列：家庭关系（离散）Wife、 Own - child、  Husband、  Not - in - family、 Other - relative、  Unmarried
  -  第九咧：种族（离散）White、  Asian - Pac - Islander、  Amer - Indian - Eskimo、  Other、 Black
  -  第十咧：性别（离散）Female、 Male.
  -  十一列：capital-gain（连续）
  -  十二列：capital-loss（连续）
  -  十三咧：每周工作时长（连续）
  -  十四列：祖国（离散）United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
  -  十五列：年收入是否超过 50K（离散） >=50K <50k

