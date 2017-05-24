# -*-coding:utf-8 -*-
import os
import logging
from gensim.matutils import MmWriter
from gensim import utils

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class MyMmWriter(MmWriter):
    u"""
    Derive from MmWriter, used for updating corpus matrix file of IFTDF model,
    there is no neccessary to read the whole mm file of corpus into RAM, overcomed the
    problems of trainning TFIDF model corpus matrix without reading the all corpus data into ram.

    This class also resolved the problem of appending new documents to serialized corpus here: 
    https://groups.google.com/forum/#!topic/gensim/5aPIl0AsLEU. the method by using "itertool.chain()"
    is not worked, because of "itertool.chain" is a iterator that only work once.
    """
    def __init__(self, fname):
        # reopen file with mode=ab+ (MmWriter open mode is wb+, will cover the data in serialized corpus),
        # append new corpus to file
        self.fname = fname
        self.fout = utils.smart_open(fname, 'ab+')
        self.last_docno = -1
        self.headers_written = True
        self.fout.seek(0, os.SEEK_END)

    @staticmethod
    def my_write_corpus(fname, corpus, docno_offset, progress_cnt=100, num_terms = None):
        mw = MyMmWriter(fname)
        mw.__get_header_info()
        _num_terms, num_nnz = mw.num_terms, 0
        docno, poslast = -1, -1
        for docno, doc in enumerate(corpus):
            bow = doc
            max_id, veclen = mw.write_vector(docno + docno_offset, bow)
            if docno % progress_cnt == 0:
                logger.info("PROGRESS: saving documemt #%i, veclen: %i" % (docno, veclen))
            _num_terms = max(_num_terms, 1 + max_id)
            num_nnz + veclen

        num_docs = docno + 1 + docno_offset
        num_terms = num_terms or _num_terms

        if num_docs * num_terms != 0:
            logger.info("saved mm file with appending new %i*%i matrix, density=%.3f%% (%i/%i)" % (
                num_docs, num_terms,
                100.0*num_nnz/(num_docs*num_terms),
                num_nnz, num_docs*num_terms))

        # now write proper headers, by seeking and overwiting the spaces written earlier
        mw.num_nnz += num_nnz
        logger.info('change matrix file header with: %i, %i, %i' % (num_docs, num_terms, mw.num_nnz))
        MyMmWriter.my_fake_header(mw, num_docs, num_terms, mw.num_nnz)

    def my_fake_header(self, num_docs, num_terms, num_nnz):
        self.fout.close()
        self.fout = utils.smart_open(self.fname, 'r+b')
        super(MyMmWriter, self).fake_headers(num_docs, num_terms, num_nnz)
        self.fout.close()
        self.fout = utils.smart_open(self.fname, 'ab+')

    def __get_header_info(self):
        with utils.file_or_filename(self.fname) as lines:
            try:
                header = utils.to_unicode(next(lines)).strip()
                if not header.lower().startswith('%%matrixmarket matrix coordinate real general'):
                    raise ValueError("File %s not in Matrix Market format with coordinate real general; instead found: \n%s" %
                        (self.fname, header))
            except StopIteration:
                logger.error(u'corpus mm file header format error | %s' % self.fname)

            self.num_docs = self.num_terms  = self.num_nnz = 0
            for lineno, line in enumerate(lines):
                line = utils.to_unicode(line)
                if not line.startswith('%'):
                    self.num_docs, self.num_terms, self.num_nnz = map(int, line.split())
                break



