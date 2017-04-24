Name:		pmysql
Version:	0.4
Release:	1%{?dist}
Summary:	Parallel MySQL client

Group:		Applications/Databases
License:	GPL	
URL:		http://facebook.com/mysqlatfacebook
Source0:	http://anywhere/%{name}-%{version}.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires: glib2-devel
BuildRequires: mysql-devel

%description
pmysql allows broadcasting a query to a large set of servers and read results from all

%prep
%setup -q

%build
env CXXFLAGS='-DMETADB=\"localinfo\"' make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT


%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
/usr/bin/pmysql
