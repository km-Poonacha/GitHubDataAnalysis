                                                                                                                                              
static int f17112(){ 
                                                                                                    
            throws SdkClientException, AmazonServiceException {
                                                                
        return getBucketWebsiteConfiguration(new GetBucketWebsiteConfigurationRequest(bucketName));
                                                                                                                                                              
    }
                                                                                                                                                                   

                                                                                                    
            throws SdkClientException, AmazonServiceException {
                                                        
        getBucketWebsiteConfigurationRequest = beforeClientExecution(getBucketWebsiteConfigurationRequest);
                  
        rejectNull(getBucketWebsiteConfigurationRequest, "The request object parameter getBucketWebsiteConfigurationRequest must be specified.");
                                                                                  
        String bucketName = getBucketWebsiteConfigurationRequest.getBucketName();
                                                                                                                                     
        rejectNull(bucketName,
                                                       
            "The bucket name parameter must be specified when requesting a bucket's website configuration");
                                                                                                                                                                   

         
        Request<GetBucketWebsiteConfigurationRequest> request = createRequest(bucketName, null, getBucketWebsiteConfigurationRequest, HttpMethodName.GET);
                                                                           
        request.addHandlerContext(HandlerContextKey.OPERATION_NAME, "GetBucketWebsite");
                                                                                                                     
        request.addParameter("website", null);
                                                                                                      
        request.addHeader("Content-Type", "application/xml");
                                                                                                                                                                   

                                                                                                                                                      
        try {
                                                  
            return invoke(request, new Unmarshallers.BucketWebsiteConfigurationUnmarshaller(), bucketName, null);
                                                                                                                     
        } catch (AmazonServiceException ase) {
                                                                                                           
            if (ase.getStatusCode() == 404) return null;
                                                                                                                                             
            throw ase;
                                                                                                                                                          
        }
                                                                                                                                                              
    }
                                                                                                                                                               
    
                                                                                                    
            throws SdkClientException, AmazonServiceException {
                                                        
        setBucketWebsiteConfiguration(new SetBucketWebsiteConfigurationRequest(bucketName, configuration));
                                                                                                                                                              
    }
                                                                                                                                                                   

                                                                                                     
           throws SdkClientException, AmazonServiceException {
                                                        
        setBucketWebsiteConfigurationRequest = beforeClientExecution(setBucketWebsiteConfigurationRequest);
                                                                                  
        String bucketName = setBucketWebsiteConfigurationRequest.getBucketName();
                                                        
        BucketWebsiteConfiguration configuration = setBucketWebsiteConfigurationRequest.getConfiguration();
                                                                                                                                                                   

                                                                                                                                     
        rejectNull(bucketName,
                                                      
                "The bucket name parameter must be specified when setting a bucket's website configuration");
                                                                                                                                  
        rejectNull(configuration,
                                     
                "The bucket website configuration parameter must be specified when setting a bucket's website configuration");
                                                                                                    
        if (configuration.getRedirectAllRequestsTo() == null) {
                                                                                                         
        rejectNull(configuration.getIndexDocumentSuffix(),
                
                "The bucket website configuration parameter must specify the index document suffix when setting a bucket's website configuration");
                                                                                                                                                          
        }
                                                                                                                                                                   

         
        Request<SetBucketWebsiteConfigurationRequest> request = createRequest(bucketName, null, setBucketWebsiteConfigurationRequest, HttpMethodName.PUT);
                                                                           
        request.addHandlerContext(HandlerContextKey.OPERATION_NAME, "PutBucketWebsite");
                                                                                                                     
        request.addParameter("website", null);
                                                                                                      
        request.addHeader("Content-Type", "application/xml");
                                                                                                                                                                   

                                                                         
        byte[] bytes = bucketConfigurationXmlFactory.convertToXmlByteArray(configuration);
                                                                                                       
        request.setContent(new ByteArrayInputStream(bytes));
                                                                                                                                                                   

                                                                                                                                                              
    }
                                                                                                                                                                   

                                                                                                    
            throws SdkClientException, AmazonServiceException {
                                                                 
        deleteBucketWebsiteConfiguration(new DeleteBucketWebsiteConfigurationRequest(bucketName));
                                                                                                                                                              
    }
                                                                                                                                                                   

                                                                                                        
        throws SdkClientException, AmazonServiceException {
                                                  
        deleteBucketWebsiteConfigurationRequest = beforeClientExecution(deleteBucketWebsiteConfigurationRequest);
                                                                               
        String bucketName = deleteBucketWebsiteConfigurationRequest.getBucketName();
                                                                                                                                                                   

                                                                                                                                     
        rejectNull(bucketName,
                                                         
            "The bucket name parameter must be specified when deleting a bucket's website configuration");
                                                                                                                                                                   


        Request<DeleteBucketWebsiteConfigurationRequest> request = createRequest(bucketName, null, deleteBucketWebsiteConfigurationRequest, HttpMethodName.DELETE);
                                                                        
        request.addHandlerContext(HandlerContextKey.OPERATION_NAME, "DeleteBucketWebsite");
                                                                                                                     
        request.addParameter("website", null);
                                                                                                      
        request.addHeader("Content-Type", "application/xml");
                                                                                                                                                                   

                                                                                                                                                              
    }
                                                                                                                                                                   

                                                                                                                                                                  
}