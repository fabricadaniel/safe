require 'rubygems'
require 'fog'

module Astrails
  module Safe
    class Glacier < Sink
        
      MAX_GLACIER_FILE_SIZE = 5368709120
      
      SECRET_KEY = 'aRHVygLm5oa/auAQaNDD4+a9r2csBk8aLAkTv40G'
      KEY = 'AKIAINEDU3HTN2KRU3MQ'  
        
      VAULT_NAME = 'bucket'  
      
      protected

      def active?
        bucket && key && secret
      end

      def path
        @path ||= expand(config[:aws, :path] || config[:local, :path] || ":kind/:id")
      end

      def save
        # FIXME: user friendly error here :)
        raise RuntimeError, "pipe-streaming not supported for S3." unless @backup.path

        # needed in cleanup even on dry run
        #AWS::S3::Base.establish_connection!(:access_key_id => key, :secret_access_key => secret, :use_ssl => true) unless local_only?

        puts "Uploading #{bucket}:#{full_path}" if verbose? || dry_run?
        unless dry_run? || local_only?
          fileSize = File.stat(@backup.path).size
          if fileSize > MAX_GLACIER_FILE_SIZE
            STDERR.puts "ERROR: File size exceeds maximum allowed for upload to S3 (#{MAX_GLACIER_FILE_SIZE}): #{@backup.path}"
            return
          end
          benchmark = Benchmark.realtime do
            glacier = Fog::AWS::Glacier.new( :access_key_id => KEY,
                                        :secret_access_key => SECRET_KEY)
            vault = glacier.vaults.create :id => VAULT_NAME

            #AWS::S3::Bucket.create(bucket) unless bucket_exists?(bucket)
            
            if glacier.vaults.get(VAULT_NAME) == nil
                vault = glacier.vaults.create :id => VAULT_NAME
            end
            
            File.open(@backup.path) do |file|
              archive1 = vault.archives.create :body => file, :multipart_chunk_size => fileSize
              puts archive1.inspect
              #AWS::S3::S3Object.store(full_path, file, bucket)
            end
          end
          puts "...done" if verbose?
          puts("Upload took " + sprintf("%.2f", benchmark) + " second(s).") if verbose?
        end
      end

      def cleanup
        return if local_only?

        return unless keep = config[:keep, :s3]

        #puts "listing files: #{bucket}:#{base}*" if verbose?
        #files = AWS::S3::Bucket.objects(bucket, :prefix => base, :max_keys => keep * 2)
        #puts files.collect {|x| x.key} if verbose?

        #files = files.
        #  collect {|x| x.key}.
        #  sort

        #cleanup_with_limit(files, keep) do |f|
        #  puts "removing s3 file #{bucket}:#{f}" if dry_run? || verbose?
        #  AWS::S3::Bucket.objects(bucket, :prefix => f)[0].delete unless dry_run? || local_only?
        #end
      end


    #--------- TODO: EDIT -----------
      def bucket
        config[:s3, :bucket]
      end

      def key
        config[:s3, :key]
      end

      def secret
        config[:s3, :secret]
      end

      private
      
      def bucket_exists?(bucket)
            true if AWS::S3::Bucket.find(bucket)
          rescue AWS::S3::NoSuchBucket
            false
      end
    #--------------------------------

      
      
    end
  end
end
