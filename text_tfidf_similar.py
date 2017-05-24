# -*-coding:utf-8 -*-
import os
import jieba
try:
    import cPikle as _pickle
except ImportError:
    import pickle as _pickle
from gensim import corpora, models, similarities, matutils

import logging

logger = logging.getLogger(__name__)

from utils.MyCorpus import MyMmWriter

PATH_STOPWORDS = './stopwords.txt'
PATH_TFIDF_FOLDER = './data'
FILENAME_CORPORA_DICTIONARY = 'corpus.dict'
FILENAME_CORPORA_SERIALIZE = 'corpus.mm'
FILENAME_INDEX = 'index'
FILENAME_DOC_IDS = 'doc_ids.arr'

NUM_BEST_SIM_DOC = 10

stopwords = set([line.strip().decode('utf-8') for line in open(PATH_STOPWORDS).readlines()])

docs = [
    '我们那一届的艺术生算上不同专业的一百人上下，有时候大家上文化课会有一些人遇到，有一些人算是点头之交，还有一些在社团活动有过来往，但是这样的人因为毕业大家从事的行业不同，加上我毕业不久后又转行了，所以联系也几乎没有。',
    '学艺术的人本来性格就比较奇怪，应酬和人情世故本来就会稍差一些，加上我自己本身就很晚熟，毕业很久还带有一种活在自己世界里的文艺气息。',
    '后来这样的酒局我就不参加了，和好友说明了情况，她甩给了我一句：你啊！就是矫情，不知道这叫多条朋友多条路么？我就笑嘻嘻地说，朋友是不能用“条”来形容的，汪汪叫的那种才能“条”形容。',
    '其实我是很怕今日大家一起吃肉，他日为财大家抢的头破血流，自古同行是冤家，同学也很难例外。'
]

def calcAbsPath(*path):
    relative_path = os.path.join(*path)
    return os.path.abspath(relative_path)

fp_corpora_dict = calcAbsPath(PATH_TFIDF_FOLDER, FILENAME_CORPORA_DICTIONARY)
fp_corpora_serialize = calcAbsPath(PATH_TFIDF_FOLDER, FILENAME_CORPORA_SERIALIZE)
fp_corpora_ids = calcAbsPath(PATH_TFIDF_FOLDER, FILENAME_DOC_IDS)
fp_index = calcAbsPath(PATH_TFIDF_FOLDER, FILENAME_INDEX)

def init():
    data_folder_path = calcAbsPath(PATH_TFIDF_FOLDER)
    if not os.path.exists(data_folder_path):
        os.makedirs(data_folder_path)
init()

def tex_similarity(docs):
    texts = []
    new_ids = [] # new docs id or name
    for id, line in enumerate(docs):
        text = [word for word in jieba.cut(line.strip(), cut_all=False) if word not in stopwords]
        texts.append(text)
        new_ids.append(id)

    if os.path.exists(fp_corpora_serialize) and os.path.exists(fp_corpora_dict) \
        and os.path.exists(fp_index) and os.path.exists(fp_corpora_ids):
        # load file
        dictionary = corpora.Dictionary.load(fp_corpora_dict)
        # get new corpus from new text and update the dictionary at the same time
        new_corpus = [dictionary.doc2bow(text, allow_update=True) for text in texts]
        # get old docs id arr
        old_doc_ids = [oId for oId in _pickle.load(open(fp_corpora_ids, 'rb'))]
        old_doc_len = len(old_doc_ids)
        new_ids = map(lambda x: x + old_doc_ids[-1], new_ids) # update the new doc id
        old_doc_ids.extend(new_ids)
        all_ids = old_doc_ids
        # append new corpus to matrix file directly, and update matrix file header info
        MyMmWriter.my_write_corpus(fp_corpora_serialize, new_corpus, old_doc_len, num_terms=len(dictionary))
        # read the appended corpus
        corpus = corpora.MmCorpus(fp_corpora_serialize)
    else:
        dictionary = corpora.Dictionary(texts)
        corpus = [dictionary.doc2bow(text) for text in texts]
        new_corpus = corpus
        # store file id
        _pickle.dump(new_ids, open(fp_corpora_ids, 'wb'), True)
        all_ids = new_ids
        matutils.MmWriter.write_corpus(fp_corpora_serialize, corpus, num_terms=len(dictionary))

    # update the new dictionary, all_ids
    dictionary.save(fp_corpora_dict)
    _pickle.dump(all_ids, open(fp_corpora_ids, 'wb'), True)  # persist the whole histroy doc ids

    logger.debug('calc text similarity | Persistence Data (dictionary, corpora, all_ids)')

    calcCorpusTFIDFSimilarity(new_ids, all_ids, corpus, new_corpus, num_feature=len(dictionary))

    logger.info('calc text similarity | total dealed text num: %i' % len(new_ids))
    return

def calcCorpusTFIDFSimilarity(new_ids, all_ids, corpus, new_corpus, num_feature=400):
    # calc IFTDF model
    tfidf = models.TfidfModel(corpus)
    crp_tfidf = tfidf[corpus]
    new_tfidf = tfidf[new_corpus]

    logger.info('corpus tfidf length: %i' % len(crp_tfidf))

    # create index
    index = similarities.Similarity(fp_index, crp_tfidf, num_features=num_feature, num_best=NUM_BEST_SIM_DOC)

    # similarity
    docs_sims = index[new_tfidf]
    index.save(fp_index)

    for idx, doc_sim in enumerate(docs_sims):
        cur_doc_id = new_ids[idx]
        logger.info(u'calc text similarity | cur_doc_id: %i | sorted_sims(0:5): %s' % (cur_doc_id, doc_sim[0:3]))



tex_similarity(docs)
