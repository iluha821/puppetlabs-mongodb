# PRIVATE CLASS: do not use directly
class mongodb::repo (
  $ensure  = $mongodb::params::ensure,
  $repo_location = undef,
) inherits mongodb::params {
  case $::osfamily {
    'RedHat', 'Linux': {
      if ($repo_location) {
        $location = $repo_location
      } else {
        $location = $::architecture ? {
          'x86_64' => 'http://downloads-distro.mongodb.org/repo/redhat/os/x86_64/',
          'i686'   => 'http://downloads-distro.mongodb.org/repo/redhat/os/i686/',
          'i386'   => 'http://downloads-distro.mongodb.org/repo/redhat/os/i686/',
          default  => undef
        }
      }
      class { 'mongodb::repo::yum': }
    }

    'Debian': {
      if ($repo_location) {
        $location = $repo_location
      } else {
        $location = $::operatingsystem ? {
          'Debian' => 'http://downloads-distro.mongodb.org/repo/debian-sysvinit',
          'Ubuntu' => 'http://downloads-distro.mongodb.org/repo/ubuntu-upstart',
          default  => undef
        }
      }
      class { 'mongodb::repo::apt': }
    }

    default: {
      if($ensure == 'present' or $ensure == true) {
        fail("Unsupported managed repository for osfamily: ${::osfamily}, operatingsystem: ${::operatingsystem}, module ${module_name} currently only supports managing repos for osfamily RedHat, Debian and Ubuntu")
      }
    }
  }
}
