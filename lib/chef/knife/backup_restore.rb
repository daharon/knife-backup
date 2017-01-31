#
# Author:: Marius Ducea (<marius.ducea@gmail.com>)
# Author:: Steven Danna (steve@opscode.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

class Chef
  class Knife
    class CookbookUpload < Knife
      def check_for_dependencies!(cookbook)
      end
    end
  end
end


module ServerBackup
  class BackupRestore < Chef::Knife

    deps do
      require 'chef/knife/core/object_loader'
      require 'chef/cookbook_uploader'
      require 'chef/api_client'
      require 'securerandom'
      require 'json'
      require 'parallel'
    end

    banner "knife backup restore [COMPONENT [COMPONENT ...]] [-D DIR] (options)"

    option :backup_dir,
      :short => "-D DIR",
      :long => "--backup-directory DIR",
      :description => "Restore backup data from DIR.",
      :default => Chef::Config[:knife][:chef_server_backup_dir] ? Chef::Config[:knife][:chef_server_backup_dir] : File.join(".chef", "chef_server_backup")

    option :ignore_metadata_errors,
      :short => "-I",
      :long => "--ignore-metadata-errors",
      :description => "Ignore json metadata errors when restoring cookbooks",
      :boolean => true,
      :default => Chef::Config[:knife][:ignore_metadata_errors] ? Chef::Config[:knife][:ignore_metadata_errors] : false

    option :concurrency,
      :short => "-C NUM_PROCS",
      :long => "--concurrency NUM_PROCS",
      :description => "The number of concurrent connections",
      :boolean => false,
      :proc => Proc.new { |value| value.to_i },
      :default => 1

    def run
      ui.warn "This will overwrite existing data!"
      ui.warn "Backup is at least 1 day old" if (Time.now - File.atime(config[:backup_dir])) > 86400
      ui.confirm "Do you want to restore backup, possibly overwriting existing data"
      validate!
      components = name_args.empty? ? COMPONENTS : name_args
      Array(components).each { |component| self.send(component) }
    end

    private
    COMPONENTS = %w(clients users nodes roles data_bags environments cookbooks)

    def validate!
      bad_names = name_args - COMPONENTS
      unless bad_names.empty?
        ui.error "Component types #{bad_names.join(",")} are not valid."
        exit 1
      end
    end

    def nodes
      restore_standard("nodes", Chef::Node)
    end

    def roles
      restore_standard("roles", Chef::Role)
    end

    def environments
      restore_standard("environments", Chef::Environment)
    end

    def data_bags
      ui.info "=== Restoring data bags ==="
      loader = Chef::Knife::Core::ObjectLoader.new(Chef::DataBagItem, ui)
      dbags = Dir.glob(File.join(config[:backup_dir], "data_bags", '*'))
      # Create the data bags.
      Parallel.map(dbags, :in_processes => config[:concurrency]) do |bag|
        bag_name = File.basename(bag)
        ui.info "Restoring data_bag[#{bag_name}]"
        begin
          rest.post_rest("data", { "name" => bag_name})
        rescue Net::HTTPServerException => e
          handle_error 'data_bag', bag_name, e
        end
      end
      # Upload the data bag items.
      data_bag_items = []
      dbags.each do |bag|
        bag_name = File.basename(bag)
        dbag_items = Dir.glob(File.join(bag, "*"))
        dbag_items.each do |item_path|
          data_bag_items << [bag_name, item_path]
        end
      end
      Parallel.map(data_bag_items, :in_processes => config[:concurrency]) do |bag, item_filename|
        item_name = File.basename(item_filename, '.json')
        ui.info "Restoring data_bag_item[#{bag}::#{item_name}]"
        item = loader.load_from("data_bags", bag, item_filename)
        dbag = Chef::DataBagItem.from_hash(item)
        dbag.data_bag(bag)
        dbag.save
        true  # Need to end with a JSON Marshallable object.
      end
    end

    def restore_standard(component, klass)
      loader = Chef::Knife::Core::ObjectLoader.new(klass, ui)
      ui.info "=== Restoring #{component} ==="
      files = Dir.glob(File.join(config[:backup_dir], component, "*.json"))
      Parallel.map(files, :in_processes => config[:concurrency]) do |f|
        new_obj = loader.load_from(component, f)
        begin
          new_obj.save
          ui.info "Updated #{component} from #{f}"
        rescue Net::HTTPNotFound
          new_obj.create
          ui.info "Created #{component} from #{f}"
        end
      end
    end

    def clients
      JSON.create_id = "no_thanks"
      ui.info "=== Restoring clients ==="
      clients = Dir.glob(File.join(config[:backup_dir], "clients", "*.json"))
      Parallel.map(clients, :in_processes => config[:concurrency]) do |file|
        client = JSON.parse(IO.read(file))
        client_obj = {
          :name => client['name'],
          :public_key => client['public_key'],
          :admin => client['admin'],
          :validator => client['validator']
        }
        begin
          rest.post_rest("clients", client_obj)
          ui.info "Created client from #{file}"
        rescue Net::HTTPServerException => e
          handle_error 'client', client['name'], e
        end
      end
    end

    def users
      require 'chef/user_v1'
      JSON.create_id = "no_thanks"
      ui.info "=== Restoring users ==="
      users = Dir.glob(File.join(config[:backup_dir], "users", "*.json"))
      Parallel.map(users, :in_processes => config[:concurrency]) do |file|
        user = Chef::UserV1.from_json(IO.read(file))
        begin
          ui.info "Restoring user #{user.username}"
          payload = {
            :username => user.username,
            :display_name => user.display_name,
            :first_name => user.first_name,
            :last_name => user.last_name,
            :email => user.email,
            :public_key => user.public_key,
            :password => '12345678'
          }
          payload[:public_key] = user.public_key unless user.public_key.nil?
          payload[:middle_name] = user.middle_name unless user.middle_name.nil?
          user.chef_root_rest_v1.post("users", payload)
        rescue Net::HTTPServerException => e
          handle_error 'user', user.username, e
        end
      end
    end

    def cookbooks
      count = 0
      ui.info "=== Restoring cookbooks ==="
      cookbooks = Dir.glob(File.join(config[:backup_dir], "cookbooks", '*'))
      Parallel.map(cookbooks, :in_processes => config[:concurrency]) do |cb|
        Dir.mktmpdir do |tmp_dir|
          full_cb = File.expand_path(cb)
          cb_name = File.basename(cb)
          cookbook = cb_name.reverse.split('-',2).last.reverse
          full_path = File.join(tmp_dir, cookbook)
          begin
            count += 1
            if Chef::Platform.windows?
              Dir.mkdir(File.join(tmp_dir, cb_name))
              full_path = File.join(tmp_dir, cb_name, cookbook)
              ui.info "Copy cookbook #{full_cb} to #{full_path}"
              FileUtils.copy_entry(full_cb, full_path)
            else
              full_path = File.join(tmp_dir, cookbook)
              File.symlink(full_cb, full_path)
            end
            cbu = Chef::Knife::CookbookUpload.new
            Chef::Knife::CookbookUpload.load_deps
            cbu.name_args = [ cookbook ]
            cbu.config[:cookbook_path] = File.dirname(full_path)
            ui.info "Restoring cookbook #{cbu.name_args}"
            cbu.run
          rescue Net::HTTPServerException => e
            handle_error('cookbook', cb_name, e)
          rescue Chef::Exceptions::JSON::ParseError => e
            handle_error('cookbook', cb_name, e)
            throw e unless config[:ignore_metadata_errors]
          end
        end
      end
      ui.info "Uploaded #{count} Cookbooks"
    end

    def handle_error(type, name, error)
      thing = "#{type}[#{name}]"
      return ui.error "Error parsing JSON for: #{thing}" if error.kind_of?(Chef::Exceptions::JSON::ParseError)

      case error.response
      when Net::HTTPConflict # 409
        ui.warn "#{thing} already exists; skipping"
      when Net::HTTPClientError # 4xx Catch All
        ui.error "Failed to create #{thing}: #{error.response}; skipping"
      else
        ui.error "Failed to create #{thing}: #{error.response}; skipping"
      end
    end

  end
end
