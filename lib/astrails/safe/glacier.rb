require 'rubygems'
require 'fog'

module Astrails
  module Safe
    class Glacier < Sink
        
      protected

      def active?
        bucket && key && secret
      end

      def path
        @path ||= expand(config[:glacier, :path] || config[:local, :path] || ":kind/:id")
      end

      def save
        # FIXME: user friendly error here :)
        raise RuntimeError, "pipe-streaming not supported for glacier." unless @backup.path

        # needed in cleanup even on dry run
        #AWS::S3::Base.establish_connection!(:access_key_id => key, :secret_access_key => secret, :use_ssl => true) unless local_only?

        puts "Uploading #{bucket}:#{full_path}" if verbose? || dry_run?
        unless dry_run? || local_only?
          fileSize = File.stat(@backup.path).size
          
          benchmark = Benchmark.realtime do
            glacier = Fog::AWS::Glacier.new(:aws_access_key_id => key,
                                            :aws_secret_access_key => secret)

            #AWS::S3::Bucket.create(bucket) unless bucket_exists?(bucket)
            
            vault = glacier.vaults.get(bucket)
            if vault == nil
                vault = glacier.vaults.create :id => bucket
            end
            
            File.open(@backup.path) do |file|
              archive1 = vault.archives.create(:body => file, 
                                               :multipart_chunk_size => 1024*1024, 
                                               :description => "backup")
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

        return unless keep = config[:keep, :glacier]

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


      def bucket
        config[:glacier, :bucket]
      end

      def key
        config[:glacier, :key]
      end

      def secret
        config[:glacier, :secret]
      end

    end
  end
end