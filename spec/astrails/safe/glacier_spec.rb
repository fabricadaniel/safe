require 'spec_helper'

describe Astrails::Safe::Glacier do

  def def_config
    {
      :glacier => {
        :bucket => "_bucket",
        :key    => "_key",
        :secret => "_secret",
      },
      :keep => {
        :glacier => 2
      }
    }
  end

  def def_backup(extra = {})
    {
      :kind      => "_kind",
      :filename  => "/backup/somewhere/_kind-_id.NOW.bar",
      :extension => ".bar",
      :id        => "_id",
      :timestamp => "NOW"
    }.merge(extra)
  end

  def glacier(config = def_config, backup = def_backup)
    Astrails::Safe::Glacier.new(
      Astrails::Safe::Config::Node.new.merge(config),
      Astrails::Safe::Backup.new(backup)
    )
  end

  describe :cleanup do

    before(:each) do
      @glacier = glacier

      @files = [4,1,3,2].map { |i| stub(o = {}).key {"aaaaa#{i}"}; o }

      #stub(AWS::S3::Bucket).objects("_bucket", :prefix => "_kind/_id/_kind-_id.", :max_keys => 4) {@files}
      #stub(AWS::S3::Bucket).objects("_bucket", :prefix => anything).stub![0].stub!.delete
    end

    it "should check [:keep, :s3]" do
      @glacier.config[:keep].data["glacier"] = nil
      dont_allow(@glacier.backup).filename
      @glacier.send :cleanup
    end

    it "should delete extra files" do
      #mock(AWS::S3::Bucket).objects("_bucket", :prefix => "aaaaa1").mock![0].mock!.delete
      #mock(AWS::S3::Bucket).objects("_bucket", :prefix => "aaaaa2").mock![0].mock!.delete
      @glacier.send :cleanup
    end

  end

  describe :active do
    before(:each) do
      @glacier = glacier
    end

    it "should be true when all params are set" do
      @glacier.should be_active
    end

    it "should be false if bucket is missing" do
      @glacier.config[:glacier].data["bucket"] = nil
      @glacier.should_not be_active
    end

    it "should be false if key is missing" do
      @glacier.config[:glacier].data["key"] = nil
      @glacier.should_not be_active
    end

    it "should be false if secret is missing" do
      @glacier.config[:glacier].data["secret"] = nil
      @glacier.should_not be_active
    end
  end

  describe :path do
    before(:each) do
      @glacier = glacier
    end
    it "should use glacier/path 1st" do
      @glacier.config[:glacier].data["path"] = "glacier_path"
      @glacier.config[:local] = {:path => "local_path"}
      @glacier.send(:path).should == "glacier_path"
    end

    it "should use local/path 2nd" do
      @glacier.config.merge local: {path: "local_path"}
      @glacier.send(:path).should == "local_path"
    end

    it "should use constant 3rd" do
      @glacier.send(:path).should == "_kind/_id"
    end

  end

  describe :save do
    def add_stubs(*stubs)
      stubs.each do |s|
        case s
        when :connection
          #stub(AWS::S3::Base).establish_connection!(:access_key_id => "_key", :secret_access_key => "_secret", :use_ssl => true)
        when :stat
          stub(File).stat("foo").stub!.size {123}
        when :create_bucket
          #stub(AWS::S3::Bucket).find('_bucket') { raise_error AWS::S3::NoSuchBucket }
          #stub(AWS::S3::Bucket).create
        when :file_open
          #stub(File).open("foo") {|f, block| block.call(:opened_file)}
        when :s3_store
          #stub(AWS::S3::S3Object).store(@full_path, :opened_file, "_bucket")
        end
      end
    end

    before(:each) do
      @glacier = glacier(def_config, def_backup(:path => "foo"))
      @full_path = "_kind/_id/backup/somewhere/_kind-_id.NOW.bar.bar"
    end

'''
    it "should fail if no backup.file is set" do
      @glacier.backup.path = nil
      proc {@glacier.send(:save)}.should raise_error(RuntimeError)
    end

    it "should establish glacier connection" do
      mock(AWS::S3::Base).establish_connection!(:access_key_id => "_key", :secret_access_key => "_secret", :use_ssl => true)
      add_stubs(:stat, :create_bucket, :file_open, :glacier_store)
      @glacier.send(:save)
    end

    it "should open local file" do
      add_stubs(:connection, :stat, :create_bucket)
      mock(File).open("foo")
      @glacier.send(:save)
    end

    it "should upload file" do
      add_stubs(:connection, :stat, :create_bucket, :file_open)
      mock(AWS::S3::S3Object).store(@full_path, :opened_file, "_bucket")
      @s3.send(:save)
    end

    it "should fail on files bigger then 5G" do
      add_stubs(:connection)
      mock(File).stat("foo").stub!.size {5*1024*1024*1024+1}
      mock(STDERR).puts(anything)
      dont_allow(Benchmark).realtime
      @s3.send(:save)
    end

    it 'should not create a bucket that already exists' do
      add_stubs(:connection, :stat, :file_open, :s3_store)
      stub(AWS::S3::Bucket).find('_bucket') { true }
      dont_allow(AWS::S3::Bucket).create
      @s3.send(:save)
    end
'''

  end
end
