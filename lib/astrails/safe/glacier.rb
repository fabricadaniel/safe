require 'rubygems'
require 'fog'

module Astrails
  module Safe
    class Glacier < Sink
        
      MAX_GLACIER_FILE_SIZE = 5368709120
      
      protected

      def active?
        bucket && key && secret
      end

      def path
        @path ||= expand(config[:aws, :path] || config[:local, :path] || ":kind/:id")
      end

      def save
        # FIXME: user friendly error here :)
        raise RuntimeError, "pipe-streaming not supported for glacer." unless @backup.path

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
            glacier = Fog::AWS::Glacier.new( :access_key_id => key,
                                        :secret_access_key => secret)

            #AWS::S3::Bucket.create(bucket) unless bucket_exists?(bucket)
            
            if glacier.vaults.get(bucket) == nil
                vault = glacier.vaults.create :id => bucket
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
