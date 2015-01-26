require 'ipaddr'
require 'ipaddr_extensions'

class IPAddr
  def prefixlen
    mask = @mask_addr
    len = 0
    len += mask & 1 and mask >>= 1 until mask == 0
    len
  end

  def to_cidr_string
    "#{to_s}/#{prefixlen}"
  end

  def as_json(options = {})
    if (ipv6? && prefixlen == 64) || (ipv4? && prefixlen == 32)
      to_s
    else
      to_cidr_string
    end
  end
end
