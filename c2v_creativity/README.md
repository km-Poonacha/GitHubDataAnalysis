# c2v_creativity
Using the code2vec to classify creativity of commit. It has files wrapper.py and cleaner.py.

### Using wrapper.py
wrapper.py runs the Code2Vec (https://github.com/tech-srl/code2vec) for any java function and returns the vectors.  

The approach used to run the wrapper: 
- git clone https://github.com/tech-srl/code2vec.git
- Install Java in your machine - https://www.java.com/en/download/help/download_options.xml
- Make sure you are using python3.7+. It may work with 3.6 but definitely not with 3.5. (I got it to run in Spyder but had issues with IDLE)
- You will have to install the following version of tensorflow: pip3 install tensorflow==2.0.0b1 There was a bug with the non beta version of tensorflow, and there was an issue raised for it after I started using the package. It may have been fixed by now. But this version works.
- Get the model in the following way:
  - wget https://s3.amazonaws.com/code2vec/model/java14m_model.tar.gz         
  - tar -xvzf java14m_model.tar.gz
- You can create a folder in the main folder with the above source code called models and place the unzipped model folder there.
- Place the wrapper.py file in the same folder.
- You can see an example of how to use the code in the if __name__ == '__main__' section. Basically, import InteractivePredictorWrapper into your code. Instatiate it as predictor=InteractivePredictorWrapper(). Then use predictor.predict(code) for a dictionary of <function_name>: <function_vector>.
- Change <input_filename = '/tmp/tmp_code.java'> to any folder in your local machine (or create a tmp folder in the active directory)
- For some reason, it seems to run only on the first try (that it the first time  you clone the repo and build the model). The second time onwards it throws this error  - "ValueError:  Variable model/WORDS_VOCAB already exists, disallowed. Did you mean to set reuse=True or reuse=tf.AUTO_REUSE in VarScope?"
  - I could override this error by forcing a reuse of the model. That is: in the file tensorflow_model.py I had to change line 267 
  -   From, 
            with tf.compat.v1.variable_scope('model', reuse=self.get_should_reuse_variables()):
  - To,
           with tf.compat.v1.variable_scope('model', reuse=True):

### Using cleaner.py



